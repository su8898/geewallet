﻿namespace GWallet.Backend.UtxoCoin

// NOTE: we can rename this file to less redundant "Account.fs" when this F# compiler bug is fixed:
// https://github.com/Microsoft/visualfsharp/issues/3231

open System
open System.Security
open System.Linq

open NBitcoin

open GWallet.Backend
open GWallet.Backend.FSharpUtil.UwpHacks

type internal TransactionOutpoint =
    {
        Transaction: Transaction;
        OutputIndex: int;
    }
    member self.ToCoin (): Coin =
        Coin(self.Transaction, uint32 self.OutputIndex)

type internal IUtxoAccount =
    inherit IAccount

    abstract member PublicKey: PubKey with get


type NormalUtxoAccount(currency: Currency, accountFile: FileRepresentation,
                       fromAccountFileToPublicAddress: FileRepresentation -> string,
                       fromAccountFileToPublicKey: FileRepresentation -> PubKey) =
    inherit GWallet.Backend.NormalAccount(currency, accountFile, fromAccountFileToPublicAddress)

    interface IUtxoAccount with
        member val PublicKey = fromAccountFileToPublicKey accountFile with get

type ReadOnlyUtxoAccount(currency: Currency, accountFile: FileRepresentation,
                         fromAccountFileToPublicAddress: FileRepresentation -> string,
                         fromAccountFileToPublicKey: FileRepresentation -> PubKey) =
    inherit GWallet.Backend.ReadOnlyAccount(currency, accountFile, fromAccountFileToPublicAddress)

    interface IUtxoAccount with
        member val PublicKey = fromAccountFileToPublicKey accountFile with get

type ArchivedUtxoAccount(currency: Currency, accountFile: FileRepresentation,
                         fromAccountFileToPublicAddress: FileRepresentation -> string,
                         fromAccountFileToPublicKey: FileRepresentation -> PubKey) =
    inherit GWallet.Backend.ArchivedAccount(currency, accountFile, fromAccountFileToPublicAddress)

    interface IUtxoAccount with
        member val PublicKey = fromAccountFileToPublicKey accountFile with get

module Account =

    let internal GetNetwork (currency: Currency) =
        if not (currency.IsUtxo()) then
            failwith <| SPrintF1 "Assertion failed: currency %A should be UTXO-type" currency
        match currency with
        | BTC -> Config.BitcoinNet
        | LTC -> Config.LitecoinNet
        | _ -> failwith <| SPrintF1 "Assertion failed: UTXO currency %A not supported?" currency

    // technique taken from https://electrumx-spesmilo.readthedocs.io/en/latest/protocol-basics.html#script-hashes
    let private GetElectrumScriptHashFromAddress (address: BitcoinAddress): string =
        let sha = NBitcoin.Crypto.Hashes.SHA256(address.ScriptPubKey.ToBytes())
        let reversedSha = sha.Reverse().ToArray()
        NBitcoin.DataEncoders.Encoders.Hex.EncodeData reversedSha

    let public GetElectrumScriptHashFromPublicAddress currency (publicAddress: string) =
        // TODO: measure how long does it take to get the script hash and if it's too long, cache it at app startup?
        BitcoinAddress.Create(publicAddress, GetNetwork currency) |> GetElectrumScriptHashFromAddress

    let internal GetPublicAddressFromPublicKey currency (publicKey: PubKey) =
        publicKey
            .GetScriptPubKey(ScriptPubKeyType.Segwit)
            .Hash
            .GetAddress(GetNetwork currency)
            .ToString()

    let internal GetPublicAddressFromNormalAccountFile (currency: Currency) (accountFile: FileRepresentation): string =
        let pubKey = PubKey(accountFile.Name)
        GetPublicAddressFromPublicKey currency pubKey

    let internal GetPublicKeyFromNormalAccountFile (accountFile: FileRepresentation): PubKey =
        PubKey accountFile.Name

    let internal GetPublicKeyFromReadOnlyAccountFile (accountFile: FileRepresentation): PubKey =
        accountFile.Content() |> PubKey

    let internal GetPublicAddressFromUnencryptedPrivateKey (currency: Currency) (privateKey: string) =
        let privateKey = Key.Parse(privateKey, GetNetwork currency)
        GetPublicAddressFromPublicKey currency privateKey.PubKey

    let internal GetAccountFromFile (accountFile: FileRepresentation) (currency: Currency) kind: IAccount =
        if not (currency.IsUtxo()) then
            failwith <| SPrintF1 "Assertion failed: currency %A should be UTXO-type" currency
        match kind with
        | AccountKind.ReadOnly ->
            ReadOnlyUtxoAccount(currency,
                                accountFile,
                                (fun accountFile -> accountFile.Name),
                                GetPublicKeyFromReadOnlyAccountFile)
                                            :> IAccount
        | AccountKind.Normal ->
            let fromAccountFileToPublicAddress = GetPublicAddressFromNormalAccountFile currency
            let fromAccountFileToPublicKey = GetPublicKeyFromNormalAccountFile
            NormalUtxoAccount(currency, accountFile,
                              fromAccountFileToPublicAddress, fromAccountFileToPublicKey)
            :> IAccount
        | _ ->
            failwith <| SPrintF1 "Kind (%A) not supported for this API" kind

    let private BalanceToShow (balances: BlockchainScriptHashGetBalanceInnerResult) =
        let unconfirmedPlusConfirmed = balances.Unconfirmed + balances.Confirmed
        let amountToShowInSatoshis =
            if unconfirmedPlusConfirmed < balances.Confirmed then
                unconfirmedPlusConfirmed
            else
                balances.Confirmed
        let amountInBtc = (Money.Satoshis amountToShowInSatoshis).ToUnit MoneyUnit.BTC
        amountInBtc

    let private BalanceMatchWithCacheOrInitialBalance address
                                                      currency
                                                      (someRetrievedBalance: BlockchainScriptHashGetBalanceInnerResult)
                                                          : bool =
        if Caching.Instance.FirstRun then
            BalanceToShow someRetrievedBalance = 0m
        else
            match Caching.Instance.TryRetrieveLastCompoundBalance address currency with
            | None -> false
            | Some balance ->
                BalanceToShow someRetrievedBalance = balance

    let private GetBalances (account: IUtxoAccount)
                            (mode: ServerSelectionMode)
                            (cancelSourceOption: Option<CustomCancelSource>)
                                : Async<BlockchainScriptHashGetBalanceInnerResult> =
        let scriptHashHex = GetElectrumScriptHashFromPublicAddress account.Currency account.PublicAddress

        let querySettings =
            QuerySettings.Balance(mode,(BalanceMatchWithCacheOrInitialBalance account.PublicAddress account.Currency))
        let balanceJob = ElectrumClient.GetBalance scriptHashHex
        Server.Query account.Currency querySettings balanceJob cancelSourceOption

    let private GetBalancesFromServer (account: IUtxoAccount)
                                      (mode: ServerSelectionMode)
                                      (cancelSourceOption: Option<CustomCancelSource>)
                                         : Async<Option<BlockchainScriptHashGetBalanceInnerResult>> =
        async {
            try
                let! balances = GetBalances account mode cancelSourceOption
                return Some balances
            with
            | ex when (FSharpUtil.FindException<ResourceUnavailabilityException> ex).IsSome ->
                return None
        }

    let internal GetShowableBalance (account: IUtxoAccount)
                                    (mode: ServerSelectionMode)
                                    (cancelSourceOption: Option<CustomCancelSource>)
                                        : Async<Option<decimal>> =
        async {
            let! maybeBalances = GetBalancesFromServer account mode cancelSourceOption
            match maybeBalances with
            | Some balances ->
                return Some (BalanceToShow balances)
            | None ->
                return None
        }

    let private ConvertToICoin (account: IUtxoAccount) (inputOutpointInfo: TransactionInputOutpointInfo): ICoin =
        let txHash = uint256 inputOutpointInfo.TransactionHash
        let scriptPubKeyInBytes = NBitcoin.DataEncoders.Encoders.Hex.DecodeData inputOutpointInfo.DestinationInHex
        let scriptPubKey = Script(scriptPubKeyInBytes)
        let coin =
            Coin(txHash, uint32 inputOutpointInfo.OutputIndex, Money(inputOutpointInfo.ValueInSatoshis), scriptPubKey)
        coin.ToScriptCoin account.PublicKey.WitHash.ScriptPubKey :> ICoin

    let private CreateTransactionAndCoinsToBeSigned (account: IUtxoAccount)
                                                    (transactionInputs: List<TransactionInputOutpointInfo>)
                                                    (destination: string)
                                                    (amount: TransferAmount)
                                                        : TransactionBuilder =
        let coins = List.map (ConvertToICoin account) transactionInputs

        let transactionBuilder = (GetNetwork account.Currency).CreateTransactionBuilder()
        transactionBuilder.AddCoins coins
        |> ignore<TransactionBuilder>

        let currency = account.Currency
        let destAddress = BitcoinAddress.Create(destination, GetNetwork currency)

        if amount.BalanceAtTheMomentOfSending <> amount.ValueToSend then
            let moneyAmount = Money(amount.ValueToSend, MoneyUnit.BTC)
            transactionBuilder.Send(destAddress, moneyAmount)
            |> ignore<TransactionBuilder>
            let originAddress = (account :> IAccount).PublicAddress
            let changeAddress = BitcoinAddress.Create(originAddress, GetNetwork currency)
            transactionBuilder.SetChange changeAddress
            |> ignore<TransactionBuilder>
        else
            transactionBuilder.SendAll destAddress
            |> ignore<TransactionBuilder>

        transactionBuilder.OptInRBF <- true

        transactionBuilder

    type internal UnspentTransactionOutputInfo =
        {
            TransactionId: string;
            OutputIndex: int;
            Value: Int64;
        }

    let private ConvertToInputOutpointInfo currency (utxo: UnspentTransactionOutputInfo)
                                               : Async<TransactionInputOutpointInfo> =
        async {
            let job = ElectrumClient.GetBlockchainTransaction utxo.TransactionId
            let! transRaw =
                Server.Query currency (QuerySettings.Default ServerSelectionMode.Fast) job None
            let transaction = Transaction.Parse(transRaw, GetNetwork currency)
            let txOut = transaction.Outputs.[utxo.OutputIndex]
            // should suggest a ToHex() method to NBitcoin's TxOut type?
            let destination = txOut.ScriptPubKey.ToHex()
            let ret = {
                TransactionHash = transaction.GetHash().ToString();
                OutputIndex = utxo.OutputIndex;
                ValueInSatoshis = txOut.Value.Satoshi;
                DestinationInHex = destination;
            }
            return ret
        }

    let rec private EstimateFees (txBuilder: TransactionBuilder)
                                 (feeRate: FeeRate)
                                 (account: IUtxoAccount)
                                 (usedInputsSoFar: List<TransactionInputOutpointInfo>)
                                 (unusedUtxos: List<UnspentTransactionOutputInfo>)
                                     : Async<Money*List<TransactionInputOutpointInfo>> =
        async {
            try
                let fees = txBuilder.EstimateFees feeRate
                return fees,usedInputsSoFar
            with
            | :? NBitcoin.NotEnoughFundsException as ex ->
                match unusedUtxos with
                | [] -> return raise <| FSharpUtil.ReRaise ex
                | head::tail ->
                    let! newInput = head |> ConvertToInputOutpointInfo account.Currency
                    let newCoin = newInput |> ConvertToICoin account
                    let newTxBuilder = txBuilder.AddCoins [newCoin]
                    let newInputs = newInput::usedInputsSoFar
                    return! EstimateFees newTxBuilder feeRate account newInputs tail
        }

    let internal EstimateFee (account: IUtxoAccount) (amount: TransferAmount) (destination: string)
                                 : Async<TransactionMetadata> = async {
        let rec addInputsUntilAmount (utxos: List<UnspentTransactionOutputInfo>)
                                      soFarInSatoshis
                                      amount
                                     (acc: List<UnspentTransactionOutputInfo>)
                                         : List<UnspentTransactionOutputInfo>*List<UnspentTransactionOutputInfo> =
            match utxos with
            | [] ->
                // should `raise InsufficientFunds` instead?
                failwith <| SPrintF2 "Not enough funds (needed: %s, got so far: %s)"
                          (amount.ToString()) (soFarInSatoshis.ToString())
            | utxoInfo::tail ->
                let newAcc =
                    // Avoid querying for zero-value UTXOs, which would make many unnecessary parallel
                    // connections to Electrum servers. (there's no need to use/consolidate zero-value UTXOs)

                    // This can be triggered on e.g. RegTest (by mining to Geewallet directly)
                    // because the block subsidy falls quickly. (it will be 0 after 7000 blocks)

                    // Zero-value OP_RETURN outputs are valid and standard:
                    // https://bitcoin.stackexchange.com/a/57103
                    if utxoInfo.Value > 0L then
                        utxoInfo::acc
                    else
                        acc

                let newSoFar = soFarInSatoshis + utxoInfo.Value
                if (newSoFar < amount) then
                    addInputsUntilAmount tail newSoFar amount newAcc
                else
                    newAcc,tail

        let job = GetElectrumScriptHashFromPublicAddress account.Currency account.PublicAddress
                  |> ElectrumClient.GetUnspentTransactionOutputs
        let! utxos = Server.Query account.Currency (QuerySettings.Default ServerSelectionMode.Fast) job None

        if not (utxos.Any()) then
            failwith "No UTXOs found!"
        let possibleInputs =
            seq {
                for utxo in utxos do
                    yield { TransactionId = utxo.TxHash; OutputIndex = utxo.TxPos; Value = utxo.Value }
            }

        // first ones are the smallest ones
        let inputsOrderedByAmount = possibleInputs.OrderBy(fun utxo -> utxo.Value) |> List.ofSeq

        let amountInSatoshis = Money(amount.ValueToSend, MoneyUnit.BTC).Satoshi
        let utxosToUse,unusedInputs =
            addInputsUntilAmount inputsOrderedByAmount 0L amountInSatoshis List.Empty

        let asyncInputs = List.map (ConvertToInputOutpointInfo account.Currency) utxosToUse
        let! inputs = Async.Parallel asyncInputs

        let initiallyUsedInputs = inputs |> List.ofArray

        let averageFee (feesFromDifferentServers: List<decimal>): decimal =
            let avg = feesFromDifferentServers.Sum() / decimal feesFromDifferentServers.Length
            avg

        //querying for 1 will always return -1 surprisingly...
        let estimateFeeJob = ElectrumClient.EstimateFee 2
        let! btcPerKiloByteForFastTrans =
            Server.Query account.Currency (QuerySettings.FeeEstimation averageFee) estimateFeeJob None

        let feeRate =
            try
                Money(btcPerKiloByteForFastTrans, MoneyUnit.BTC) |> FeeRate
            with
            | ex ->
                // we need more info in case this bug shows again: https://gitlab.com/knocte/geewallet/issues/43
                raise <| Exception(SPrintF1 "Could not create fee rate from %s btc per KB"
                                           (btcPerKiloByteForFastTrans.ToString()), ex)

        let transactionBuilder = CreateTransactionAndCoinsToBeSigned account
                                                                     initiallyUsedInputs
                                                                     destination
                                                                     amount

        try
            let! estimatedMinerFee,allUsedInputs =
                EstimateFees transactionBuilder feeRate account initiallyUsedInputs unusedInputs

            let estimatedMinerFeeInSatoshis = estimatedMinerFee.Satoshi
            let minerFee = MinerFee(estimatedMinerFeeInSatoshis, DateTime.UtcNow, account.Currency)

            return { Inputs = allUsedInputs; Fee = minerFee }
        with
        | :? NBitcoin.NotEnoughFundsException ->
            return raise <| InsufficientBalanceForFee None
    }

    let private SignTransactionWithPrivateKey (account: IUtxoAccount)
                                              (txMetadata: TransactionMetadata)
                                              (destination: string)
                                              (amount: TransferAmount)
                                              (privateKey: Key) =

        let btcMinerFee = txMetadata.Fee

        let finalTransactionBuilder = CreateTransactionAndCoinsToBeSigned account txMetadata.Inputs destination amount

        finalTransactionBuilder.AddKeys privateKey |> ignore
        finalTransactionBuilder.SendFees (Money.Satoshis btcMinerFee.EstimatedFeeInSatoshis)
        |> ignore<TransactionBuilder>

        let finalTransaction = finalTransactionBuilder.BuildTransaction true
        let transCheckResultAfterSigning = finalTransaction.Check()
        if (transCheckResultAfterSigning <> TransactionCheckResult.Success) then
            failwith <| SPrintF1 "Transaction check failed after signing with %A" transCheckResultAfterSigning

        let success, errors = finalTransactionBuilder.Verify finalTransaction
        if not success then
            failwith <| SPrintF1 "Something went wrong when verifying transaction: %A" errors
        finalTransaction

    let internal GetPrivateKey (account: NormalAccount) password =
        let encryptedPrivateKey = account.GetEncryptedPrivateKey()
        let encryptedSecret = BitcoinEncryptedSecretNoEC(encryptedPrivateKey, GetNetwork (account:>IAccount).Currency)
        try
            encryptedSecret.GetKey(password)
        with
        | :? SecurityException ->
            raise (InvalidPassword)

    let internal SignTransaction (account: NormalUtxoAccount)
                                 (txMetadata: TransactionMetadata)
                                 (destination: string)
                                 (amount: TransferAmount)
                                 (password: string) =

        let privateKey = GetPrivateKey account password

        let signedTransaction = SignTransactionWithPrivateKey
                                    account
                                    txMetadata
                                    destination
                                    amount
                                    privateKey
        let rawTransaction = signedTransaction.ToHex()
        rawTransaction

    let internal CheckValidPassword (account: NormalAccount) (password: string) =
        GetPrivateKey account password |> ignore

    let private BroadcastRawTransaction currency (rawTx: string): Async<string> =
        let job = ElectrumClient.BroadcastTransaction rawTx
        Server.Query currency QuerySettings.Broadcast job None

    let internal BroadcastTransaction currency (transaction: SignedTransaction<_>) =
        // FIXME: stop embedding TransactionInfo element in SignedTransaction<BTC>
        // and show the info from the RawTx, using NBitcoin to extract it
        BroadcastRawTransaction currency transaction.RawTransaction

    let internal SendPayment (account: NormalUtxoAccount)
                             (txMetadata: TransactionMetadata)
                             (destination: string)
                             (amount: TransferAmount)
                             (password: string)
                    =
        let baseAccount = account :> IAccount
        if (baseAccount.PublicAddress.Equals(destination, StringComparison.InvariantCultureIgnoreCase)) then
            raise DestinationEqualToOrigin

        let finalTransaction = SignTransaction account txMetadata destination amount password
        BroadcastRawTransaction baseAccount.Currency finalTransaction

    // TODO: maybe move this func to Backend.Account module, or simply inline it (simple enough)
    let public ExportUnsignedTransactionToJson trans =
        Marshalling.Serialize trans

    let internal SaveUnsignedTransaction (transProposal: UnsignedTransactionProposal)
                                         (txMetadata: TransactionMetadata)
                                         (readOnlyAccounts: seq<ReadOnlyAccount>)
                                             : string =

        let unsignedTransaction =
            {
                Proposal = transProposal;
                Cache = Caching.Instance.GetLastCachedData().ToDietCache readOnlyAccounts;
                Metadata = txMetadata;
            }
        ExportUnsignedTransactionToJson unsignedTransaction

    let internal SweepArchivedFunds (account: ArchivedUtxoAccount)
                                    (balance: decimal)
                                    (destination: IAccount)
                                    (txMetadata: TransactionMetadata)
                                        =
        let currency = (account:>IAccount).Currency
        let network = GetNetwork currency
        let amount = TransferAmount(balance, balance, currency)
        let privateKey = Key.Parse(account.GetUnencryptedPrivateKey(), network)
        let signedTrans = SignTransactionWithPrivateKey
                              account txMetadata destination.PublicAddress amount privateKey
        BroadcastRawTransaction currency (signedTrans.ToHex())

    let internal Create currency (password: string) (seed: array<byte>): Async<FileRepresentation> =
        async {
            use privKey = new Key (seed)
            let network = GetNetwork currency
            let secret = privKey.GetBitcoinSecret network
            let encryptedSecret = secret.PrivateKey.GetEncryptedBitcoinSecret(password, network)
            let encryptedPrivateKey = encryptedSecret.ToWif()
            let publicKey = secret.PubKey.ToString()
            return {
                Name = publicKey
                Content = fun _ -> encryptedPrivateKey
            }
        }

    let internal ValidateAddress (currency: Currency) (address: string) =
        if String.IsNullOrEmpty address then
            raise <| ArgumentNullException "address"

        let BITCOIN_ADDRESS_BECH32_PREFIX = "bc1"
        let LITECOIN_ADDRESS_BECH32_PREFIX = "ltc1"

        let utxoCoinValidAddressPrefixes =
            match currency with
            | BTC ->
                let BITCOIN_ADDRESS_PUBKEYHASH_PREFIX = "1"
                let BITCOIN_ADDRESS_SCRIPTHASH_PREFIX = "3"
                [
                    BITCOIN_ADDRESS_PUBKEYHASH_PREFIX
                    BITCOIN_ADDRESS_SCRIPTHASH_PREFIX
                    BITCOIN_ADDRESS_BECH32_PREFIX
                ]
            | LTC ->
                let LITECOIN_ADDRESS_PUBKEYHASH_PREFIX = "L"
                let LITECOIN_ADDRESS_SCRIPTHASH_PREFIX = "M"
                [
                    LITECOIN_ADDRESS_PUBKEYHASH_PREFIX
                    LITECOIN_ADDRESS_SCRIPTHASH_PREFIX
                    LITECOIN_ADDRESS_BECH32_PREFIX
                ]
            | _ -> failwith <| SPrintF1 "Unknown UTXO currency %A" currency

        if not (utxoCoinValidAddressPrefixes.Any(fun prefix -> address.StartsWith prefix)) then
            raise (AddressMissingProperPrefix(utxoCoinValidAddressPrefixes))

        let allowedAddressLength: AddressLength =
            match currency, address with
            | Currency.BTC, _ when address.StartsWith BITCOIN_ADDRESS_BECH32_PREFIX ->
                // taken from https://github.com/bitcoin/bips/blob/master/bip-0173.mediawiki
                // (FIXME: this is only valid for the first version of segwit, fix it!)
                Fixed [ 42u; 62u ]
            | Currency.LTC, _ when address.StartsWith LITECOIN_ADDRESS_BECH32_PREFIX ->
                // taken from https://coin.space/all-about-address-types/, e.g. ltc1q3qkpj5s4ru3cx9t7dt27pdfmz5aqy3wplamkns
                // FIXME: hopefully someone replies/documents https://bitcoin.stackexchange.com/questions/110975/how-long-can-bech32-addresses-be-in-the-litecoin-mainnet
                Fixed [ 43u ]
            | _ ->
                Variable { Minimum = 27u; Maximum = 34u }

        match allowedAddressLength with
        | Fixed allowedLengths ->
            if not (allowedLengths.Select(fun uLen -> int uLen).Contains address.Length) then
                raise <| AddressWithInvalidLength allowedAddressLength
        | Variable { Minimum = min; Maximum = max } ->
            if address.Length > int max then
                raise <| AddressWithInvalidLength allowedAddressLength
            if address.Length < int min then
                raise <| AddressWithInvalidLength allowedAddressLength

        let network = GetNetwork currency
        try
            BitcoinAddress.Create(address, network)
            |> ignore<BitcoinAddress>
        with
        // TODO: propose to NBitcoin upstream to generate an NBitcoin exception instead
        | :? FormatException ->
            raise (AddressWithInvalidChecksum None)

    let GetSignedTransactionDetails<'T when 'T :> IBlockchainFeeInfo>(rawTransaction: string)
                                                                     (currency: Currency)
                                                                         : ITransactionDetails =
        let network = GetNetwork currency
        match Transaction.TryParse(rawTransaction, network) with
        | false, _ ->
            failwith "malformed transaction"
        | true, transaction ->
            let txInGetSigner (txIn: TxIn): IDestination =
                let signer = txIn.GetSigner()
                if Object.ReferenceEquals(signer, null) then
                    failwith "unable to determine signer"
                else
                    signer

            let origin =
                if transaction.Inputs.Count = 0 then
                    failwith "transaction has no inputs"
                let origin = txInGetSigner transaction.Inputs.[0]
                for txIn in Seq.skip 1 transaction.Inputs do
                    let thisOrigin = txInGetSigner txIn
                    if origin <> thisOrigin then
                        failwith "transaction has multiple different inputs"
                origin

            let matchOriginToAccount(account: ReadOnlyUtxoAccount): bool =
                let accountAddress = (account :> IAccount).PublicAddress
                let bitcoinAddress = BitcoinAddress.Create(accountAddress, network)
                let destination = bitcoinAddress.ScriptPubKey.GetDestination()
                (destination :> IDestination) = origin

            let account =
                let accountOpt =
                    Config.GetAccountFiles [currency] AccountKind.ReadOnly
                    |> Seq.map
                        (fun accountFile ->
                            GetAccountFromFile accountFile currency AccountKind.ReadOnly
                            :?> ReadOnlyUtxoAccount
                        )
                    |> Seq.filter matchOriginToAccount
                    |> Seq.tryExactlyOne
                match accountOpt with
                | Some account -> account
                | None -> failwith "unknown origin account"
            let originAddress =
                let accountAddress = (account :> IAccount).PublicAddress
                let bitcoinAddress = BitcoinAddress.Create(accountAddress, network)
                bitcoinAddress

            let destinationAddress, value =
                let filterChangeTxOuts(txOut: TxOut): Option<BitcoinAddress * Money> =
                    let scriptPubKey = txOut.ScriptPubKey
                    let destinationAddress = scriptPubKey.GetDestinationAddress network
                    let destination = destinationAddress.ScriptPubKey.GetDestination()
                    if (destination :> IDestination) = origin then
                        None
                    else
                        Some (destinationAddress, txOut.Value)

                let filteredTxOuts = Seq.choose filterChangeTxOuts transaction.Outputs
                match Seq.tryExactlyOne filteredTxOuts with
                | Some destinationAddress -> destinationAddress
                | None ->
                    failwith "expected a single destination address"

            {
                OriginAddress = originAddress.ToString()
                DestinationAddress = destinationAddress.ToString()
                Amount = value.ToDecimal MoneyUnit.BTC
                Currency = currency
            } :> ITransactionDetails

