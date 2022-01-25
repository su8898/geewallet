namespace GWallet.Backend.UtxoCoin

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
open GWallet.Backend.FSharpUtil
open GWallet.Backend.FSharpUtil.UwpHacks

module internal TorOperations =
    let internal GetTorDirectory retryCount =
        async {
            let rec tryGetDirectory currentRetryCount =
                async {
                    try
                        return! TorDirectory.Bootstrap (FallbackDirectorySelector.GetRandomFallbackDirectory())
                    with
                    | :? NOnion.NOnionException as ex ->
                        if currentRetryCount = 0 then
                            return raise <| FSharpUtil.ReRaise ex
                        return! tryGetDirectory (currentRetryCount - 1)
                }
            return! tryGetDirectory retryCount
        }

    let internal StartTorServiceHost directory retryCount =
        async {
            let rec tryStart currentRetryCount =
                async {
                    try
                        let host = TorServiceHost(directory, retryCount)
                        do! host.Start()
                        return host
                    with
                    | :? NOnion.NOnionException as ex ->
                        if currentRetryCount = 0 then
                            return raise <| FSharpUtil.ReRaise ex
                        return! tryStart (currentRetryCount - 1)
                }
            return! tryStart retryCount
        }

    let internal TorConnect directory retryCount introductionPointPublicInfo =
        async {
            let rec tryConnect currentRetryCount =
                async {
                    try
                        return! TorServiceClient.Connect directory introductionPointPublicInfo
                    with
                    | :? NOnion.NOnionException as ex ->
                        if currentRetryCount = 0 then
                            return raise <| FSharpUtil.ReRaise ex
                        return! tryConnect (currentRetryCount - 1)
                }
            return! tryConnect retryCount
        }
