﻿namespace GWallet.Backend.UtxoCoin.Lightning

open System
open System.IO

open DotNetLightning.Channel
open NBitcoin

open GWallet.Backend
open GWallet.Backend.UtxoCoin
open GWallet.Backend.FSharpUtil
open GWallet.Backend.FSharpUtil.UwpHacks

type FundingBroadcastButNotLockedData =
    {
        Currency: Currency
        TxId: TransactionIdentifier
        MinimumDepth: uint32
    }
    member self.GetRemainingConfirmations(): Async<uint32> =
        async {
            let! confirmationCount =
                UtxoCoin.Server.Query
                    self.Currency
                    (UtxoCoin.QuerySettings.Default ServerSelectionMode.Fast)
                    (UtxoCoin.ElectrumClient.GetConfirmations (self.TxId.ToString()))
                    None
            if confirmationCount < self.MinimumDepth then
                let remainingConfirmations = self.MinimumDepth - confirmationCount
                return remainingConfirmations
            else
                return 0u
        }

type LocallyForceClosedData = {
    Network: Network
    Currency: Currency
    ToSelfDelay: uint16
    SpendingTransactionString: string
} with
    member self.GetRemainingConfirmations(): Async<uint16> = async {
        let spendingTransaction = Transaction.Parse(self.SpendingTransactionString, self.Network)
        let forceCloseTxId =
            let txIn = Seq.exactlyOne spendingTransaction.Inputs
            txIn.PrevOut.Hash
        let! confirmationCount =
            UtxoCoin.Server.Query
                self.Currency
                (UtxoCoin.QuerySettings.Default ServerSelectionMode.Fast)
                (UtxoCoin.ElectrumClient.GetConfirmations (forceCloseTxId.ToString()))
                None
        if confirmationCount < uint32 self.ToSelfDelay then
            let remainingConfirmations = self.ToSelfDelay - uint16 confirmationCount
            return remainingConfirmations
        else
            return 0us
    }


type ChannelStatus =
    | FundingBroadcastButNotLocked of FundingBroadcastButNotLockedData
    | Closing
    | Closed
    | LocallyForceClosed of LocallyForceClosedData
    | Active
    | Broken

type ChannelInfo =
    {
        ChannelId: ChannelIdentifier
        IsFunder: bool
        Balance: decimal
        SpendableBalance: decimal
        Capacity: decimal
        MaxBalance: decimal
        MinBalance: decimal
        FundingTxId: TransactionIdentifier
        FundingOutPointIndex: uint32
        Status: ChannelStatus
        Currency: Currency
    }
    static member internal FromSerializedChannel (serializedChannel: SerializedChannel)
                                                 (currency: Currency)
                                                     : ChannelInfo = {
        ChannelId = ChannelSerialization.ChannelId serializedChannel
        IsFunder = ChannelSerialization.IsFunder serializedChannel
        Balance = (ChannelSerialization.Balance serializedChannel).ToMoney().ToUnit NBitcoin.MoneyUnit.BTC
        SpendableBalance = (ChannelSerialization.SpendableBalance serializedChannel).ToMoney().ToUnit NBitcoin.MoneyUnit.BTC
        Capacity = (ChannelSerialization.Capacity serializedChannel).ToUnit NBitcoin.MoneyUnit.BTC
        MaxBalance = (ChannelSerialization.MaxBalance serializedChannel).ToMoney().ToUnit NBitcoin.MoneyUnit.BTC
        MinBalance = (ChannelSerialization.MinBalance serializedChannel).ToMoney().ToUnit NBitcoin.MoneyUnit.BTC
        FundingTxId =
            TransactionIdentifier.FromHash (ChannelSerialization.Commitments serializedChannel).FundingScriptCoin.Outpoint.Hash
        FundingOutPointIndex = (ChannelSerialization.Commitments serializedChannel).FundingScriptCoin.Outpoint.N
        Currency = currency
        Status =
            match serializedChannel.LocalForceCloseSpendingTxOpt with
            | Some localForceCloseSpendingTx ->
                ChannelStatus.LocallyForceClosed {
                    Network = serializedChannel.Network
                    Currency = currency
                    ToSelfDelay = (ChannelSerialization.Commitments serializedChannel).LocalParams.ToSelfDelay.Value
                    SpendingTransactionString = localForceCloseSpendingTx
                }
            | None ->
                match serializedChannel.ChanState with
                | ChannelState.Negotiating _
                | ChannelState.Closing _ ->
                    Closing
                | ChannelState.Closed _ ->
                    Closed
                | ChannelState.Normal _ -> ChannelStatus.Active
                | ChannelState.WaitForFundingConfirmed waitForFundingConfirmedData ->
                    let txId = TransactionIdentifier.FromHash waitForFundingConfirmedData.Commitments.FundingScriptCoin.Outpoint.Hash
                    let minimumDepth = serializedChannel.MinSafeDepth.Value
                    let fundingBroadcastButNotLockedData = {
                        Currency = currency
                        TxId = txId
                        MinimumDepth = minimumDepth
                    }
                    ChannelStatus.FundingBroadcastButNotLocked fundingBroadcastButNotLockedData
                | _ -> ChannelStatus.Broken
    }

type ChannelStore(account: NormalUtxoAccount) =
    static member ChannelFilePrefix = "chan-"
    static member ChannelFileEnding = ".json"

    member val Account = account
    member val Currency = (account :> IAccount).Currency

    member self.AccountDir: DirectoryInfo =
        Config.GetConfigDir self.Currency AccountKind.Normal

    member self.ChannelDir: DirectoryInfo =
        let subdirectory = SPrintF1 "%s-lightning" (self.Account :> BaseAccount).AccountFile.Name
        Path.Combine (self.AccountDir.FullName, subdirectory) |> DirectoryInfo

    member self.ListChannelIds(): seq<ChannelIdentifier> =
        let extractChannelId path: Option<ChannelIdentifier> =
            let fileName = Path.GetFileName path
            let withoutPrefix = fileName.Substring ChannelStore.ChannelFilePrefix.Length
            let withoutEnding =
                withoutPrefix.Substring(
                    0,
                    withoutPrefix.Length - ChannelStore.ChannelFileEnding.Length
                )
            ChannelIdentifier.Parse withoutEnding

        if self.ChannelDir.Exists then
            let files =
                Directory.GetFiles self.ChannelDir.FullName
            files |> Seq.choose extractChannelId
        else
            Seq.empty

    member self.ChannelFileName (channelId: ChannelIdentifier): string =
        Path.Combine(
            self.ChannelDir.FullName,
            SPrintF3
                "%s%s%s"
                ChannelStore.ChannelFilePrefix
                (channelId.ToString())
                ChannelStore.ChannelFileEnding
        )

    member internal self.LoadChannel (channelId: ChannelIdentifier): SerializedChannel =
        let fileName = self.ChannelFileName channelId
        let json = File.ReadAllText fileName
        Marshalling.DeserializeCustom<SerializedChannel> (
            json,
            SerializedChannel.LightningSerializerSettings self.Currency
        )

    member internal self.SaveChannel (serializedChannel: SerializedChannel) =
        let fileName = self.ChannelFileName (ChannelSerialization.ChannelId serializedChannel)
        let json =
            Marshalling.SerializeCustom(
                serializedChannel,
                SerializedChannel.LightningSerializerSettings self.Currency
            )
        if not self.ChannelDir.Exists then
            self.ChannelDir.Create()
        File.WriteAllText(fileName, json)

    member self.ChannelInfo (channelId: ChannelIdentifier): ChannelInfo =
        let serializedChannel = self.LoadChannel channelId
        ChannelInfo.FromSerializedChannel serializedChannel self.Currency

    member self.ListChannelInfos(): seq<ChannelInfo> = seq {
        for channelId in self.ListChannelIds() do
            let channelInfo = self.ChannelInfo channelId
            if channelInfo.Status <> ChannelStatus.Closing &&
               channelInfo.Status <> ChannelStatus.Closed then
                yield channelInfo
    }

    member self.DeleteChannel (channelId: ChannelIdentifier): unit =
        let fileName = self.ChannelFileName channelId
        File.Delete fileName

    member self.GetCommitmentTx (channelId: ChannelIdentifier): string =
        let commitments =
            let serializedChannel = self.LoadChannel channelId
            UnwrapOption
                serializedChannel.ChanState.Commitments
                "A channel can only end up in the wallet if it has commitments."
        commitments.LocalCommit.PublishableTxs.CommitTx.Value.ToHex()

    member self.CheckForClosingTx (channelId: ChannelIdentifier)
                                      : Async<Option<string * Option<uint32>>> = async {
        let serializedChannel = self.LoadChannel channelId
        let commitments = ChannelSerialization.Commitments serializedChannel
        let currency = self.Currency
        let network = UtxoCoin.Account.GetNetwork currency
        let fundingAddressString: string =
            let fundingAddress: BitcoinAddress =
                let fundingDestination: TxDestination =
                    commitments.FundingScriptCoin.ScriptPubKey.GetDestination()
                fundingDestination.GetAddress network
            fundingAddress.ToString()
        let scriptHash = Account.GetElectrumScriptHashFromPublicAddress currency fundingAddressString
        let! historyList =
            Server.Query
                currency
                (QuerySettings.Default ServerSelectionMode.Fast)
                (ElectrumClient.GetBlockchainScriptHashHistory scriptHash)
                None
        let fundingTxId = TransactionIdentifier.FromHash (ChannelSerialization.Commitments serializedChannel).FundingScriptCoin.Outpoint.Hash
        Console.WriteLine(SPrintF1 "** WOW ** - fundingTxId == %A" fundingTxId)
        Console.WriteLine(SPrintF1 "** WOW ** - historyList == %A" historyList)
        let! balance =
            Server.Query
                currency
                (QuerySettings.Default ServerSelectionMode.Fast)
                (ElectrumClient.GetBalance scriptHash)
                None
        Console.WriteLine(SPrintF1 "** WOW ** - balance == %A" balance)

        if historyList.Length = 2 then
            let closingTxId = historyList.[1].TxHash
            let closingTxHeightOpt =
                let reportedHeight = historyList.[1].Height
                if reportedHeight = 0u then
                    None
                else
                    Some reportedHeight
            return Some (closingTxId, closingTxHeightOpt)
        else
            return None
    }

module ChannelManager =
    // difference from fee estimation in UtxoCoinAccount.fs: this is for P2WSH
    let EstimateChannelOpeningFee (account: UtxoCoin.NormalUtxoAccount) (amount: TransferAmount) =
        let witScriptIdLength = 32
        // this dummy address is only used for fee estimation
        let nullScriptId = NBitcoin.WitScriptId (Array.zeroCreate witScriptIdLength)
        let network = UtxoCoin.Account.GetNetwork (account :> IAccount).Currency
        let dummyAddr = NBitcoin.BitcoinWitScriptAddress (nullScriptId, network)
        UtxoCoin.Account.EstimateFeeForDestination account amount dummyAddr

