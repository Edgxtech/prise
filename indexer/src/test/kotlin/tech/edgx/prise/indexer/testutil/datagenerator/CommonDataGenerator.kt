package tech.edgx.prise.indexer.testutil.datagenerator

import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.helper.reactive.BlockStreamer
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Ignore
import org.junit.jupiter.api.Test
import org.koin.core.parameter.parametersOf
import org.koin.test.inject
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.Base
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.processor.SwapProcessor
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.classifier.module.WingridersClassifier
import tech.edgx.prise.indexer.testutil.TransactionBodyExcludeStrategy
import java.io.File
import java.io.PrintWriter

class CommonDataGenerator: Base() {
    private val log = LoggerFactory.getLogger(javaClass)
    val transactionBodyGson: Gson = GsonBuilder().addDeserializationExclusionStrategy(TransactionBodyExcludeStrategy()).create()
    val chainService: ChainService by inject { parametersOf(config) }
    val swapProcessor: SwapProcessor by inject { parametersOf(config) }

    @Ignore
    @Test
    fun saveSpecificQualifiedTransactionToJson() {
        //val txHash = "fa30415a3d5e336b80d940d880461bcf19cd1c63b2981ccbe51c1f30557feb82"; val txSlot = 132239546L
        //val txHash = "d3aba39861706b25c5a8c33ce48889be998405a55b646002ee2f085e5a9fcd14"; val txSlot = 132222216L
        //val txHash = "bee485180e6ae05af621ac0e560fbfaf57b8563f81e95b960930129d12ecdd94"; val txSlot = 132476966L
        //val txHash = "e0b5b5e5d18878c9b502c0d39f9b813d6fc0f2ace3e2dbb98bd97ee19c298872"; val txSlot = 132459047L
        //val txHash = "592ac794f7ea60aba502b44aa436178626bef087ce7f05bf4a53dbd1456287be"; val txSlot = 132219880L
        //val txHash = "fa30415a3d5e336b80d940d880461bcf19cd1c63b2981ccbe51c1f30557feb82"; val txSlot = 132239546L
        //val txHash = "72875a21809e7c75d0e98e4751171eafc66847dc2614fa70208cfc067d565d90"; val txSlot = 116200902L
        //val txHash = "b02042417e8fa4e2386b0e47c85e3f2d18e3483196c861767758ccba52e75730"; val txSlot = 116978969L
        //val txHash = "3cae4bea2849f1cc8546a96058320f0deb949289cbd58295cfc7f50dab71f15a"; val txSlot = 112501470L
//        val txHash = "24983065abb54ff66368fda2c32372325bac4a1320452fd7643210699e76c6ae"; val txSlot = 130934066L
        //val txHash = "d3aba39861706b25c5a8c33ce48889be998405a55b646002ee2f085e5a9fcd14"; val txSlot = 132222216L
        //val txHash = "269a8408bb1d47087a164267fcc6488dd65754d31b9c0f1547e63d6850ed35a4"; val txSlot = 132208958L
        //val txHash = "505bd29029f181a40f2e6d6c59a3628086d6161c358a59e08f0d253b90f8097b"; val txSlot = 132266099L
        //val txHash = "5a164d5248026e39dc14912eab0434af786001a2c38674c5a34e8cb601abd204"
        val txHash = "f4d58892c029fd778982ea87c66a07c713c5a4975bbc1855ef637c143fc1553c"
        val dexName = DexEnum.MINSWAPV2.nativeName
        /** Need to update this as block prior to block containing requested tx **/
        val startPoint = Point(130907257, "240689ad58ebc9dcb58da3af6a60fd423d28600c30ad21b443ea56cc39c2e14e")

        val outputFile = "src/test/resources/testdata/$dexName/transaction_qualified_${txHash}.json"

        val writer: PrintWriter = File(outputFile).printWriter()
        var processedBlock = false
        val blockStreamer = BlockStreamer.fromPoint(
            config.cnodeAddress,
            config.cnodePort!!,
            startPoint,
            NetworkType.MAINNET.n2NVersionTable
        )
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            log.info("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")
            val qualifiedTx = swapProcessor.qualifyTransactions(
                block.header.headerBody.slot,
                block.transactionBodies,
                block.transactionWitness
            )
            val filteredDexSpecificTx = qualifiedTx
                .firstOrNull { it.txHash == txHash }
            println("Found transactions for DEX #: ${filteredDexSpecificTx}")

            if (filteredDexSpecificTx != null) {
                writer.println(Gson().toJson(filteredDexSpecificTx))
                writer.flush()
                writer.close()

            }
            processedBlock = true
        }
        runBlocking {
            while (true) {
                if (processedBlock) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }
    }

    /* Helper. Generate raw blocks for reuse in testing */
    @Ignore
    @Test
    fun saveRawBlocksToJson_FromChainsync() {
        //val point = Point(115585887, "08cafda5d8757773cc2a7fac8c0c19653413ef01a11fd7f2966cd23cf8dfee10")
        //val point = Point(115688784, "e9e25ce047f544033644ecbe32dba26554eb25f98c92ca5abe48d53e107fe034") // prior to erroneous swap cal ETH0.5, https://cardanoscan.io/transaction/a988fb387810671ed56d9b9a286b84ea31b3c15033129596725f70cba155a558?tab=utxo
        //val point = Point(115667964,"afdb94fa19acb040eab85619273d2ebadcc81a2b24b0d69426fc2e8f8b901938") // prior to buggy data
        //val point = Point(112587104, "3f5985f4172fbcff021d47ca55c8adec2572425205ee1c70183a20809c7f5e96") // Point prior to where is a tx with a duel op; buy and sell op (i.e. tx: 3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e)
        //val point = Point(112587070, "3763de0e50e1c86f929581642254aac30074225b1bd05aa4f3ed80d99647cab3") // prior to where there is a normal single swap
        //val point = Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b") // priot to a point where there is a single swap
        val startPoint = Point(115585887, "08cafda5d8757773cc2a7fac8c0c19653413ef01a11fd7f2966cd23cf8dfee10") // priot to a point where there is a single swap

        val numberOfTransactionsToSave = 100
        val writer: PrintWriter = File("src/test/resources/testdata/blocks_from_112501875_100blocks.json").printWriter()
        val allBlocks = mutableListOf<Block>()
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            log.info("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")
            allBlocks.add(block)
            // .cbor feature not yet added (https://github.com/bloxbean/yaci/issues/66)
            // val tx = Transaction.deserialize(CborSerializationUtil.deserialize(HexUtil.decodeHexString(block.transactionBodies[0].cbor)))
            println("Running tx total #: ${allBlocks.size}")
        }
        runBlocking {
            while(true) {
                if (allBlocks.size >= numberOfTransactionsToSave) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }
        writer.println(Gson().toJson(allBlocks))
        writer.flush()
        writer.close()
    }

    @Ignore
    @Test
    fun saveQualifiedTransactionsToJson_FromChainSync() {
        val numberOfTransactionsToSave = 1

        //val point = Point(115585887, "08cafda5d8757773cc2a7fac8c0c19653413ef01a11fd7f2966cd23cf8dfee10")
        //val point = Point(115688784, "e9e25ce047f544033644ecbe32dba26554eb25f98c92ca5abe48d53e107fe034") // prior to erroneous swap cal ETH0.5, https://cardanoscan.io/transaction/a988fb387810671ed56d9b9a286b84ea31b3c15033129596725f70cba155a558?tab=utxo
        //val point = Point(115667964,"afdb94fa19acb040eab85619273d2ebadcc81a2b24b0d69426fc2e8f8b901938") // prior to buggy data
        //val point = Point(112587104, "3f5985f4172fbcff021d47ca55c8adec2572425205ee1c70183a20809c7f5e96") // prior to where is a tx with a duel op; buy and sell op (i.e. tx: 3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e)
        //val point = Point(112587070, "3763de0e50e1c86f929581642254aac30074225b1bd05aa4f3ed80d99647cab3") // prior to where there is a normal single swap
        //val point = Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b") // prior to a point where there is a single swap
        //val startPoint = Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b") // prior to a point where there is a single swap
        //val startPoint = Point(112541716, "ca96dbfd879fae1fa48717fabe3e0d313460c3e574247b6f1f58cc8f073e4036") // prior to minswap tx
        //val startPoint = Point(112553452, "d2c5b21453a68bd91fb4251fbf420fd0affba21374cdc0d2a4177ea76e9a4844") // prior to minswap tx
        //val startPoint = Point(112503994, "77a240d6353137086cc8a191a30502f85cc72c7d29e4caa5f2aa9cfffb04e304") // prior to sundaeswap tx
        //val startPoint = Point(112501957, "b1ce9d6f21e19d9eb3b9872f0561bdc044d2b38f8f527b98fe641e3cdc1f4347") // prior to sundaeswap tx (3swaps)
        val startPoint = Point(131365380, "e84ae6722d7ca9f438930f38ac992741cdc803324c57d6fcc9fb7a49dfaf6197")

        /* ALL DEXES */
        //val dexes = listOf(0,1,2)
        //val outputFile = "src/test/resources/testdata/transactions_swaps_from_block_${startPoint.slot}.json"

        /* WINGRIDERS */
//        val dexes = listOf(0)
//        val outputFile = "src/test/resources/testdata/wingriders/transactions_qualified_from_block_${startPoint.slot}.json"

        /* SUNDAESWAP */
//        val dexes = listOf(1)
//        val outputFile = "src/test/resources/testdata/sundaeswap/transactions_qualified_from_block_${startPoint.slot}.json"

        /* MINSWAP */
//        val dexes = listOf(2)
//        val outputFile = "src/test/resources/testdata/minswap/transactions_qualified_from_block_${startPoint.slot}.json"

        /* SATURNSWAP */
        val dexes = listOf(3)
        val outputFile = "src/test/resources/testdata/saturnswap/transactions_qualified_from_block_${startPoint.slot}.json"

        val writer: PrintWriter = File(outputFile).printWriter()
        val allTxDTOs = mutableListOf<FullyQualifiedTxDTO>()
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            log.info("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")
            val qualifiedTx = swapProcessor.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
            val filteredDexSpecificTx = qualifiedTx
                .filter { dexes.contains(it.dexCode) } // SPECIFIC DEXES
            allTxDTOs.addAll(filteredDexSpecificTx)
            println("Running qualified tx total #: ${allTxDTOs.size}")
        }
        runBlocking {
            while(true) {
                if (allTxDTOs.size >= numberOfTransactionsToSave) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }
        writer.println(Gson().toJson(allTxDTOs))
        writer.flush()
        writer.close()
    }

    /* Helper. Generate fully qualified transactions for qualified DEX swaps only */
    @Test
    fun saveQualifiedTransactionsToJson_FromBlocks() {
        /* ALL DEXES */
//        val dexes = listOf(0,1,2)
//        val inputFile = "src/test/resources/testdata/blocks_from_9894905.json"
//        val outputFile = "src/test/resources/testdata/transactions_qualified_from_block_9894905.json"

        /* WINGRIDERS */
//        val dexes = listOf(0)
//        val inputFile = "src/test/resources/testdata/blocks_from_9894905_10tx.json"
//        val outputFile = "src/test/resources/testdata/wingriders/transactions_qualified_from_block_9894905.json"

//        /* SUNDAESWAP */
//        val dexes = listOf(1)
//        val inputFile = "src/test/resources/testdata/blocks_from_9894905_10tx.json"
//        val outputFile = "src/test/resources/testdata/sundaeswap/transactions_qualified_from_block_9894905.json"

        /* MINSWAP */
        val dexes = listOf(2)
        val inputFile = "src/test/resources/testdata/blocks_from_9894905_10tx.json"
        val outputFile = "src/test/resources/testdata/minswap/transactions_qualified_from_block_9894905.json"

        val writer: PrintWriter = File(outputFile).printWriter()
        val blocks: List<Block> = transactionBodyGson.fromJson<List<Block>>(
            File(inputFile).readText(Charsets.UTF_8).byteInputStream().bufferedReader().readLine(),
            object : TypeToken<ArrayList<Block>>() {}.type)
        val qualifiedTx: List<FullyQualifiedTxDTO> = blocks.flatMap {
            swapProcessor.qualifyTransactions(it.header.headerBody.slot, it.transactionBodies, it.transactionWitness)
        }.filter { dexes.contains(it.dexCode) } // SPECIFIC DEXES
        println("QualifiedTx, #: ${qualifiedTx.size}")
        writer.println(Gson().toJson(qualifiedTx))
        writer.flush()
        writer.close()
    }

    /* Helper. Convert qualified transactions to swaps */
    @Ignore
    @Test
    fun saveSwapsToJson() {
        /* WINGRIDERS */
//        val outputFile = "src/test/resources/testdata/wingriders/swaps_from_block_9894905.json"
//        val inputFile = "src/test/resources/testdata/wingriders/transactions_qualified_from_block_9894905.json"
//        val outputFile = "src/test/resources/testdata/wingriders/swaps_from_block_112501875_500tx.json"
//        val inputFile = "src/test/resources/testdata/wingriders/transactions_qualified_from_block_112501875_500tx.json"

//        /* SUNDAESWAP */
//        val outputFile = "src/test/resources/testdata/sundaeswap/swaps_from_block_112501875_500tx.json"
//        val inputFile = "src/test/resources/testdata/sundaeswap/transactions_qualified_from_block_112501875_500tx.json"

        /* MINSWAP */
        val outputFile = "src/test/resources/testdata/minswap/swaps_from_block_112501875_500tx.json"
        val inputFile = "src/test/resources/testdata/minswap/transactions_qualified_from_block_112501875_500tx.json"

        val writer: PrintWriter = File(outputFile).printWriter()
        /* Use pre-saved transaction data; generating in ChainServiceIT */
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson(
            File(inputFile).readText(Charsets.UTF_8).byteInputStream().bufferedReader().readLine(),
            object : TypeToken<List<FullyQualifiedTxDTO?>?>() {}.getType())
        println("Parsed # txDTOs: ${txDTOs.size}")
        val swaps = txDTOs.flatMap { WingridersClassifier.computeSwaps(it)}
        writer.println(Gson().toJson(swaps))
        writer.flush()
        writer.close()
    }
}