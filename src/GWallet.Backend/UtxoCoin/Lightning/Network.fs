namespace GWallet.Backend.UtxoCoin.Lightning

open System
open System.Net.Sockets
open System.Net
open System.Diagnostics
open System.IO

open NBitcoin
open DotNetLightning.Peer
open DotNetLightning.Utils
open ResultUtils.Portability
open NOnion.Network
open NOnion.Directory
open NOnion.Services

open GWallet.Backend
open GWallet.Backend.UtxoCoin
open GWallet.Backend.FSharpUtil
open GWallet.Backend.FSharpUtil.UwpHacks

type PeerDisconnectedError =
    {
        Abruptly: bool
    }
    interface IErrorMsg with
        member self.Message =
            if self.Abruptly then
                "peer disconnected after sending a partial message"
            else
                "peer disconnected"
        member __.ChannelBreakdown =
            false

    member internal self.PossibleBug =
        not self.Abruptly

type HandshakeError =
    | TcpConnect of seq<SocketException>
    | TcpAccept of seq<SocketException>
    | DisconnectedOnAct1 of PeerDisconnectedError
    | InvalidAct1 of PeerError
    | DisconnectedOnAct2 of PeerDisconnectedError
    | InvalidAct2 of PeerError
    | DisconnectedOnAct3 of PeerDisconnectedError
    | InvalidAct3 of PeerError
    interface IErrorMsg with
        member self.Message =
            match self with
            | TcpConnect errs ->
                let messages = Seq.map (fun (err: SocketException) -> err.Message) errs
                SPrintF1 "TCP connection failed: %s" (String.concat "; " messages)
            | TcpAccept errs ->
                let messages = Seq.map (fun (err: SocketException) -> err.Message) errs
                SPrintF1 "TCP accept failed: %s" (String.concat "; " messages)
            | DisconnectedOnAct1 err ->
                SPrintF1 "Peer disconnected before starting handshake: %s" (err :> IErrorMsg).Message
            | InvalidAct1 err ->
                SPrintF1 "Invalid handshake act 1: %s" err.Message
            | DisconnectedOnAct2 err ->
                SPrintF1 "Peer disconnected before sending handshake act 2: %s" (err :> IErrorMsg).Message
            | InvalidAct2 err ->
                SPrintF1 "Invalid handshake act 2: %s" err.Message
            | DisconnectedOnAct3 err ->
                SPrintF1 "Peer disconnected before sending handshake act 3: %s" (err :> IErrorMsg).Message
            | InvalidAct3 err ->
                SPrintF1 "Invalid handshake act 3: %s" err.Message
        member __.ChannelBreakdown =
            false

    member internal self.PossibleBug =
        match self with
        | DisconnectedOnAct1 _
        | DisconnectedOnAct2 _
        | DisconnectedOnAct3 _ -> false
        | TcpConnect _
        | TcpAccept _
        | InvalidAct1 _
        | InvalidAct2 _
        | InvalidAct3 _ -> false

type RecvBytesError =
    | PeerDisconnected of PeerDisconnectedError
    | Decryption of PeerError
    interface IErrorMsg with
        member self.Message =
            match self with
            | PeerDisconnected err ->
                SPrintF1 "Peer disconnected: %s" (err :> IErrorMsg).Message
            | Decryption err ->
                SPrintF1 "Error decrypting message from peer: %s" err.Message
        member self.ChannelBreakdown: bool =
            match self with
            | PeerDisconnected peerDisconnectedError -> (peerDisconnectedError :> IErrorMsg).ChannelBreakdown
            | Decryption _ -> true

    member internal self.PossibleBug =
        match self with
        | PeerDisconnected err -> err.PossibleBug
        | Decryption _ -> false

type ListenerType =
    | Tcp of TcpListener
    | Tor of TorServiceHost

type internal TransportListener =
    internal {
        NodeMasterPrivKey: NodeMasterPrivKey
        Listener: ListenerType
        NodeServerType: NodeServerType
        ConnectionDetail: Option<IntroductionPointPublicInfo>
    }
    interface IDisposable with
        member self.Dispose() =
            match self.Listener with
            | ListenerType.Tcp tcp ->
                tcp.Stop()
            | ListenerType.Tor _tor ->
                // TODO: stop the TorServiceHost (how do we do that?)
                ()

    static member internal Bind (nodeMasterPrivKey: NodeMasterPrivKey)
                                (mayBeEndpoint: Option<IPEndPoint>)
                                (nodeServerType: NodeServerType) = async {
        match nodeServerType with
        | NodeServerType.Tcp ->
            match mayBeEndpoint with
            | Some endpoint ->
                let listener = new TcpListener (endpoint)
                listener.ExclusiveAddressUse <- false
                listener.Server.SetSocketOption(
                    SocketOptionLevel.Socket,
                    SocketOptionName.ReuseAddress,
                    true
                )
                Console.WriteLine "start server 3"
                listener.Start()
                Console.WriteLine "start server 4"

                return {
                    NodeMasterPrivKey = nodeMasterPrivKey
                    Listener = ListenerType.Tcp listener
                    NodeServerType = nodeServerType
                    ConnectionDetail = None
                }
            | None -> return failwith "tcp endpoint cannot be null"
        | NodeServerType.Tor ->
            // TODO: where must this come from? is the number of retries arbitrary?
            let retryCount = 10
            let! directory = TorOperations.GetTorDirectory retryCount
            let! host = TorOperations.StartTorServiceHost directory retryCount

            let exportedConnectionDetail = host.Export()
            
            return {
                NodeMasterPrivKey = nodeMasterPrivKey
                Listener = ListenerType.Tor host
                NodeServerType = nodeServerType
                ConnectionDetail = Some exportedConnectionDetail
            }
        }

    member internal self.LocalIPEndPoint: Option<IPEndPoint> =
        match self.Listener with
        | ListenerType.Tcp tcp ->
            // sillly .NET API: downcast is even done in the sample from the docs: https://docs.microsoft.com/en-us/dotnet/api/system.net.sockets.tcplistener.localendpoint?view=netcore-3.1
            Some (tcp.LocalEndpoint :?> IPEndPoint)
        | ListenerType.Tor _tor ->
            // TODO: IPEndPoint of TorServiceHost?
            None

    member internal self.NodeId: NodeId =
        self.NodeMasterPrivKey.NodeId()

    member internal self.PubKey: PubKey =
        self.NodeId.Value

    member internal self.EndPoint: Option<NodeEndPoint> =
        match self.LocalIPEndPoint with
            | Some localIpEndpoint ->
                let nodeId = PublicKey self.PubKey
                Some (NodeEndPoint.FromParts nodeId localIpEndpoint)
            | _ -> None

    member internal self.TorEndPoint: Option<NodeNOnionIntroductionPoint> =
        let nodeId = PublicKey self.PubKey
        // TODO: refactor. shouldn't use value and shouldn't return Option
        Some ({ NodeNOnionIntroductionPoint.NodeId = nodeId; IntroductionPointPublicInfo = self.ConnectionDetail.Value })

type PeerErrorMessage =
    {
        ErrorMsg: DotNetLightning.Serialization.Msgs.ErrorMsg
    }
    interface IErrorMsg with
        member self.Message =
            if self.ErrorMsg.Data.Length = 1 then
                let code = self.ErrorMsg.Data.[0]
                (SPrintF1 "Error code %i received from lightning peer: " code) +
                match code with
                | 0x01uy ->
                    "The number of pending channels exceeds the policy limit.\n\
                    Hint: You can try from a new node identity."
                | 0x02uy ->
                    "Node is not synced to blockchain." +
                    if Config.BitcoinNet() = Network.RegTest then
                        "\nHint: Try mining some blocks before opening."
                    else
                        String.Empty
                | 0x03uy ->
                    "Channel capacity too large.\n\
                    Hint: Try with a smaller funding amount."
                | _ ->
                    "(unknown error code)"
            else
                System.Text.ASCIIEncoding.ASCII.GetString self.ErrorMsg.Data

        member __.ChannelBreakdown: bool =
            true

type TransportStreamClientType =
    | Client of TcpClient
    | TorClientStream of TorStream

type StreamType =
| TcpNetworkStream of NetworkStream
| TorClientStream of TorStream

type internal TransportStream =
    internal {
        NodeMasterPrivKey: NodeMasterPrivKey
        Peer: Peer
        Client: TransportStreamClientType
    }
    interface IDisposable with
        member self.Dispose() =
            match self.Client with
            | TransportStreamClientType.Client tcpClient ->
                tcpClient.Close()
            | TransportStreamClientType.TorClientStream _torStream ->
                (*
                    FIXME: torStream.End() is an async method so it cannot be called here.
                    self.Close() method must be called in order to close/end the torStream.
                *)
                ()

    member self.Close() = async {
        match self.Client with
        | TransportStreamClientType.Client _tcpClient ->
            ()
        | TransportStreamClientType.TorClientStream torStream ->
            do! torStream.End()
    }

    static member private bolt08EncryptedMessageLengthPrefixLength = 18
    static member private bolt08EncryptedMessageMacLength = 16
    // https://github.com/lightningnetwork/lightning-rfc/blob/master/08-transport.md#authenticated-key-exchange-handshake-specification
    static member private bolt08ActOneLength = 50
    static member private bolt08ActTwoLength = 50
    static member private bolt08ActThreeLength = 66

    static member private ReadExactAsync (stream: StreamType)
                                         (numberBytesToRead: int)
                                             : Async<Result<array<byte>, PeerDisconnectedError>> =
        let buf: array<byte> = Array.zeroCreate numberBytesToRead
        let rec read buf totalBytesRead =
            let readAsync () =
                async {
                    match stream with
                    | StreamType.TcpNetworkStream tcpStream ->
                        return! tcpStream.ReadAsync(buf, totalBytesRead, numberBytesToRead - totalBytesRead) |> Async.AwaitTask
                    | StreamType.TorClientStream torStream ->
                        return! torStream.Receive buf totalBytesRead (numberBytesToRead - totalBytesRead)
                }
            async {
                let! maybeBytesRead =
                    async {
                        try
                            let! res = readAsync ()
                            return Some res
                        with
                        | ex ->
                            if (FSharpUtil.FindException<System.Net.Sockets.SocketException> ex).IsSome then
                                return None
                            else
                                return raise <| FSharpUtil.ReRaise ex
                    }

                match maybeBytesRead with
                | Some bytesRead ->
                    let totalBytesRead = totalBytesRead + bytesRead
                    if bytesRead = 0 then
                        if totalBytesRead = 0 then
                            return Error { Abruptly = false }
                        else
                            return Error { Abruptly = true }
                    else
                        if totalBytesRead < numberBytesToRead then
                            return! read buf totalBytesRead
                        else
                            return Ok buf
                | None -> return Error { Abruptly = true }
            }
        read buf 0

    static member private TcpOrTorConnect (localEndPointOpt: Option<IPEndPoint>)
                                        (remoteEndPoint: IPEndPoint)
                                        (node: NodeIdentifier)
                                         : Async<Result<TransportStreamClientType, seq<SocketException>>> = async {
        match node with
        | NodeIdentifier.EndPoint _endpoint ->
            let client = new TcpClient (remoteEndPoint.AddressFamily)
            match localEndPointOpt with
            | Some localEndPoint ->
                client.Client.ExclusiveAddressUse <- false
                client.Client.SetSocketOption(
                    SocketOptionLevel.Socket,
                    SocketOptionName.ReuseAddress,
                    true
                )
                client.Client.Bind(localEndPoint)
                Infrastructure.LogDebug <| SPrintF2 "Connecting over TCP from %A to %A..." localEndPoint remoteEndPoint
            | None ->
                Infrastructure.LogDebug <| SPrintF1 "Connecting over TCP to %A..." remoteEndPoint
            try
                do! client.ConnectAsync(remoteEndPoint.Address, remoteEndPoint.Port) |> Async.AwaitTask
                return Ok (TransportStreamClientType.Client client)
            with
            | ex ->
                client.Close()
                let socketExceptions = FindSingleException<SocketException> ex
                return Error socketExceptions
        | NodeIdentifier.NOnionIntroductionPoint nonionIntroductionPoint ->
            let introductionIpEndPoint = ((IPAddress.Parse nonionIntroductionPoint.IntroductionPointPublicInfo.Address, nonionIntroductionPoint.IntroductionPointPublicInfo.Port) |> IPEndPoint)
            Infrastructure.LogDebug <| SPrintF1 "Connecting over TOR to %A..." introductionIpEndPoint

            // TODO: where must this come from? is the number of retries arbitrary?
            let retryCount = 10
            let! directory = TorOperations.GetTorDirectory retryCount
            try
                let! torClient = TorOperations.TorConnect directory retryCount nonionIntroductionPoint.IntroductionPointPublicInfo
                Infrastructure.LogDebug <| SPrintF1 "Connected %s" nonionIntroductionPoint.IntroductionPointPublicInfo.Address
                return Ok (TransportStreamClientType.TorClientStream (torClient.GetStream()))
            with
            | ex ->
                let socketExceptions = FindSingleException<SocketException> ex
                return Error socketExceptions
            }

    static member private TcpOrTorAcceptAny (listener: ListenerType)
                                               : Async<Result<TransportStreamClientType, seq<SocketException>>> = async {
        try
            match listener with
            | ListenerType.Tcp tcpListener ->
                let! client = tcpListener.AcceptTcpClientAsync() |> Async.AwaitTask
                return Ok (TransportStreamClientType.Client client)
            | ListenerType.Tor torListener ->
                let! client = torListener.AcceptClient()
                return Ok (TransportStreamClientType.TorClientStream client)
        with
        | ex ->
            let socketExceptions = FindSingleException<SocketException> ex
            return Error socketExceptions
    }

    static member private ConnectHandshake (tcpOrTorclient: TransportStreamClientType)
                                           (nodeMasterPrivKey: NodeMasterPrivKey)
                                           (peerNodeId: NodeId)
                                           (peerId: PeerId)
                                               : Async<Result<TransportStream, HandshakeError>> = async {
        match tcpOrTorclient with
        | TransportStreamClientType.Client client ->
            let nodeSecret = nodeMasterPrivKey.NodeSecret()
            let stream = client.GetStream()
            // FIXME: CreateOutbound should take a NodeSecret
            let peer = Peer.CreateOutbound(peerId, peerNodeId, nodeSecret.RawKey())
            let act1, peerEncryptor = PeerChannelEncryptor.getActOne peer.ChannelEncryptor
            Debug.Assert((TransportStream.bolt08ActOneLength = act1.Length), "act1 has wrong length")
            let peerAfterAct1 = { peer with ChannelEncryptor = peerEncryptor }

            // Send act1
            do! stream.WriteAsync(act1, 0, act1.Length) |> Async.AwaitTask

            // Receive act2
            Infrastructure.LogDebug "Receiving Act 2..."
            let! act2Res = TransportStream.ReadExactAsync (StreamType.TcpNetworkStream stream) TransportStream.bolt08ActTwoLength
            match act2Res with
            | Error peerDisconnectedError -> return Error <| DisconnectedOnAct2 peerDisconnectedError
            | Ok act2 ->
                let peerCmd = ProcessActTwo(act2, nodeSecret.RawKey())
                match Peer.executeCommand peerAfterAct1 peerCmd with
                | Error err -> return Error <| InvalidAct2 err
                | Ok (ActTwoProcessed ((act3, _), _) as evt::[]) ->
                    let peerAfterAct2 = Peer.applyEvent peerAfterAct1 evt

                    Debug.Assert((TransportStream.bolt08ActThreeLength = act3.Length), SPrintF1 "act3 has wrong length (not %i)" TransportStream.bolt08ActThreeLength)

                    do! stream.WriteAsync(act3, 0, act3.Length) |> Async.AwaitTask
                    return Ok {
                        NodeMasterPrivKey = nodeMasterPrivKey
                        Peer = peerAfterAct2
                        Client = TransportStreamClientType.Client client
                    }
                | Ok evts ->
                    return failwith <| SPrintF1
                        "DNL returned unexpected events when processing act2: %A" evts
        | TransportStreamClientType.TorClientStream stream ->
            let nodeSecret = nodeMasterPrivKey.NodeSecret()
            // FIXME: CreateOutbound should take a NodeSecret
            let peer = Peer.CreateOutbound(peerId, peerNodeId, nodeSecret.RawKey())
            let act1, peerEncryptor = PeerChannelEncryptor.getActOne peer.ChannelEncryptor
            Debug.Assert((TransportStream.bolt08ActOneLength = act1.Length), "act1 has wrong length")
            let peerAfterAct1 = { peer with ChannelEncryptor = peerEncryptor }

            // Send act1
            do! stream.SendData(act1)

            // Receive act2
            Infrastructure.LogDebug "Receiving Act 2..."
            let! act2Res = TransportStream.ReadExactAsync (StreamType.TorClientStream stream) TransportStream.bolt08ActTwoLength
            match act2Res with
            | Error peerDisconnectedError -> return Error <| DisconnectedOnAct2 peerDisconnectedError
            | Ok act2 ->
                let peerCmd = ProcessActTwo(act2, nodeSecret.RawKey())
                match Peer.executeCommand peerAfterAct1 peerCmd with
                | Error err -> return Error <| InvalidAct2 err
                | Ok (ActTwoProcessed ((act3, _), _) as evt::[]) ->
                    let peerAfterAct2 = Peer.applyEvent peerAfterAct1 evt

                    Debug.Assert((TransportStream.bolt08ActThreeLength = act3.Length), SPrintF1 "act3 has wrong length (not %i)" TransportStream.bolt08ActThreeLength)

                    let! _sendResAct3 = stream.SendData(act3)
                    return Ok {
                        NodeMasterPrivKey = nodeMasterPrivKey
                        Peer = peerAfterAct2
                        Client = TransportStreamClientType.TorClientStream stream
                    }
                | Ok evts ->
                    return failwith <| SPrintF1
                        "DNL returned unexpected events when processing act2: %A" evts
    }

    static member internal Connect
        (nodeMasterPrivKey: NodeMasterPrivKey)
        (nodeIdentifier: NodeIdentifier)
        : Async<Result<TransportStream, HandshakeError>> = async {

        let ipEndPoint, nodeId =
            match nodeIdentifier with
            | NodeIdentifier.EndPoint nodeEndPoint ->
                nodeEndPoint.IPEndPoint, (nodeEndPoint.NodeId.ToString() |> NBitcoin.PubKey |> NodeId)
            | NodeIdentifier.NOnionIntroductionPoint nonionIntroductionPoint ->
                // FIXME: FallbackDirectorySelector.GetRandomFallbackDirectory() is only used to construct the peerId below.
                // https://gitlab.com/su8898/geewallet/-/commit/7889b89ea1af02b331ad73c84887647ff0445b26#note_812553965
                FallbackDirectorySelector.GetRandomFallbackDirectory(), (nonionIntroductionPoint.NodeId.ToString() |> NBitcoin.PubKey |> NodeId)
        let peerId = PeerId (ipEndPoint :> EndPoint)

        let peerEndpoint = ipEndPoint
        let! connectRes = TransportStream.TcpOrTorConnect None peerEndpoint nodeIdentifier
        match connectRes with
        | Error err -> return Error <| TcpConnect err
        | Ok client ->
            return! TransportStream.ConnectHandshake client nodeMasterPrivKey nodeId peerId
    }

    static member internal AcceptFromTransportListener (transportListener: TransportListener)
                                                           : Async<Result<TransportStream, HandshakeError>> = async {
        let! clientRes = TransportStream.TcpOrTorAcceptAny transportListener.Listener
        match clientRes with
        | Error socketError -> return Error <| TcpAccept socketError
        | Ok transportClient ->
            let nodeSecret = transportListener.NodeMasterPrivKey.NodeSecret()
            let nodeSecretKey = nodeSecret.RawKey()
            match transportClient with
            | TransportStreamClientType.Client client ->
                let stream = client.GetStream()
                let peerId = client.Client.RemoteEndPoint |> PeerId
                let peer = Peer.CreateInbound(peerId, nodeSecretKey)
                let! act1Res = TransportStream.ReadExactAsync (StreamType.TcpNetworkStream stream) TransportStream.bolt08ActOneLength
                match act1Res with
                | Error peerDisconnectedError -> return Error <| DisconnectedOnAct1 peerDisconnectedError
                | Ok act1 ->
                    let peerCmd = ProcessActOne(act1, nodeSecretKey)
                    match Peer.executeCommand peer peerCmd with
                    | Error err -> return Error <| InvalidAct1 err
                    | Ok (ActOneProcessed(act2, _) as evt::[]) ->
                        let peerAfterAct2 = Peer.applyEvent peer evt
                        do! stream.WriteAsync(act2, 0, act2.Length) |> Async.AwaitTask
                        let! act3Res = TransportStream.ReadExactAsync (StreamType.TcpNetworkStream stream) TransportStream.bolt08ActThreeLength
                        match act3Res with
                        | Error peerDisconnectedError ->
                            return Error <| DisconnectedOnAct3 peerDisconnectedError
                        | Ok act3 ->
                            let peerCmd = ProcessActThree act3
                            match Peer.executeCommand peerAfterAct2 peerCmd with
                            | Error err -> return Error <| InvalidAct3 err
                            | Ok (ActThreeProcessed(_, _) as evt::[]) ->
                                let peerAfterAct3 = Peer.applyEvent peerAfterAct2 evt

                                return Ok {
                                    NodeMasterPrivKey = transportListener.NodeMasterPrivKey
                                    Peer = peerAfterAct3
                                    Client = TransportStreamClientType.Client client
                                }
                            | Ok evts ->
                                return failwith <| SPrintF1
                                    "DNL returned unexpected events when processing act3: %A" evts
                    | Ok evts ->
                        return failwith <| SPrintF1
                            "DNL returned unexpected events when processing act1: %A" evts
            | TransportStreamClientType.TorClientStream stream ->
                let peerId = ((IPAddress.Parse "127.0.0.1", 1234) |> IPEndPoint :> EndPoint ) |> PeerId
                let peer = Peer.CreateInbound(peerId, nodeSecretKey)
                let! act1Res = TransportStream.ReadExactAsync (StreamType.TorClientStream stream) TransportStream.bolt08ActOneLength
                match act1Res with
                | Error peerDisconnectedError -> return Error <| DisconnectedOnAct1 peerDisconnectedError
                | Ok act1 ->
                    let peerCmd = ProcessActOne(act1, nodeSecretKey)
                    match Peer.executeCommand peer peerCmd with
                    | Error err -> return Error <| InvalidAct1 err
                    | Ok (ActOneProcessed(act2, _) as evt::[]) ->
                        let peerAfterAct2 = Peer.applyEvent peer evt
                        do! stream.SendData(act2)
                        let! act3Res = TransportStream.ReadExactAsync (StreamType.TorClientStream stream) TransportStream.bolt08ActThreeLength
                        match act3Res with
                        | Error peerDisconnectedError ->
                            return Error <| DisconnectedOnAct3 peerDisconnectedError
                        | Ok act3 ->
                            let peerCmd = ProcessActThree act3
                            match Peer.executeCommand peerAfterAct2 peerCmd with
                            | Error err -> return Error <| InvalidAct3 err
                            | Ok (ActThreeProcessed(_, _) as evt::[]) ->
                                let peerAfterAct3 = Peer.applyEvent peerAfterAct2 evt

                                return Ok {
                                    NodeMasterPrivKey = transportListener.NodeMasterPrivKey
                                    Peer = peerAfterAct3
                                    Client = TransportStreamClientType.TorClientStream (stream)
                                }
                            | Ok evts ->
                                return failwith <| SPrintF1
                                    "DNL returned unexpected events when processing act3: %A" evts
                    | Ok evts ->
                        return failwith <| SPrintF1
                            "DNL returned unexpected events when processing act1: %A" evts
    }

    member internal self.RemoteNodeId
        with get(): NodeId =
            match self.Peer.TheirNodeId with
            | Some nodeId ->
                nodeId
            | None -> 
                failwith
                    "The TransportStream type is created by performing a handshake \
                    and in doing so guarantees that the peer's node id is known"

    member internal self.PeerId
        with get(): PeerId = self.Peer.PeerId

    member internal self.RemoteEndPoint
        with get(): Option<IPEndPoint> =
            match self.Client with
            | TransportStreamClientType.Client tcpClient ->
                Some (tcpClient.Client.RemoteEndPoint :?> IPEndPoint)
            | TransportStreamClientType.TorClientStream _torClient ->
                None

    member internal self.NodeEndPoint: Option<NodeEndPoint> =
        match self.RemoteEndPoint with
        | Some remoteEndPoint ->
            let remoteNodeId = PublicKey self.RemoteNodeId.Value
            Some (NodeEndPoint.FromParts remoteNodeId remoteEndPoint)
        | _ -> None

    member internal self.SendBytes (plaintext: array<byte>): Async<TransportStream> = async {
        let peer = self.Peer
        let ciphertext, channelEncryptor =
            PeerChannelEncryptor.encryptMessage plaintext peer.ChannelEncryptor
        let peerAfterBytesSent = { peer with ChannelEncryptor = channelEncryptor }
        match self.Client with
        | TransportStreamClientType.Client tcpClient ->
            let stream = tcpClient.GetStream()
            do! stream.WriteAsync(ciphertext, 0, ciphertext.Length) |> Async.AwaitTask
        | TransportStreamClientType.TorClientStream stream ->
            do! stream.SendData ciphertext
        return { self with Peer = peerAfterBytesSent }
    }

    member internal self.RecvBytes(): Async<Result<TransportStream * array<byte>, RecvBytesError>> = async {
        let peer = self.Peer
        let stream =
            match self.Client with
            | TransportStreamClientType.Client tcpClient ->
                StreamType.TcpNetworkStream (tcpClient.GetStream())
            | TransportStreamClientType.TorClientStream stream ->
                StreamType.TorClientStream stream

        let! encryptedLengthRes =
            TransportStream.ReadExactAsync stream TransportStream.bolt08EncryptedMessageLengthPrefixLength
        match encryptedLengthRes with
        | Error peerDisconnectedError -> return Error <| PeerDisconnected peerDisconnectedError
        | Ok encryptedLength ->
            let decryptLengthRes =
                PeerChannelEncryptor.decryptLengthHeader encryptedLength peer.ChannelEncryptor
            match decryptLengthRes with
            | Error peerError -> return Error <| Decryption peerError
            | Ok (length, channelEncryptor) ->
                let! ciphertextRes =
                    TransportStream.ReadExactAsync
                        stream
                        (int length + TransportStream.bolt08EncryptedMessageMacLength)
                match ciphertextRes with
                | Error _peerDisconnectedError -> return Error <| PeerDisconnected { Abruptly = true }
                | Ok ciphertext ->
                    let decryptBodyRes =
                        PeerChannelEncryptor.decryptMessage ciphertext channelEncryptor
                    match decryptBodyRes with
                    | Error peerError -> return Error <| Decryption peerError
                    | Ok (plaintext, channelEncryptor) ->
                        let peerAfterBytesReceived = { peer with ChannelEncryptor = channelEncryptor }
                        let transportStream = { self with Peer = peerAfterBytesReceived }
                        return Ok (transportStream, plaintext)
    }

