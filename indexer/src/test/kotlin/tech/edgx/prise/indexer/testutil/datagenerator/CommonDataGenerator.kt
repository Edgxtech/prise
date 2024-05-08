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
import tech.edgx.prise.indexer.service.BaseIT
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.classifier.module.WingridersClassifier
import tech.edgx.prise.indexer.testutil.TransactionBodyExcludeStrategy
import java.io.File
import java.io.PrintWriter

class CommonDataGenerator: BaseIT() {
    private val log = LoggerFactory.getLogger(javaClass)
    val transactionBodyGson: Gson = GsonBuilder().addDeserializationExclusionStrategy(TransactionBodyExcludeStrategy()).create()
    val chainService: ChainService by inject { parametersOf(config) }

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

        val numberOfTransactionsToSave = 10
        val writer: PrintWriter = File("src/test/resources/testdata/blocks_from_9894905.json").printWriter()
        val allBlocks = mutableListOf<Block>()
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            log.info("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")
            allBlocks.add(block)
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
        //val numberOfTransactionsToSave = 1
        val numberOfTransactionsToSave = 500

        //val point = Point(115585887, "08cafda5d8757773cc2a7fac8c0c19653413ef01a11fd7f2966cd23cf8dfee10")
        //val point = Point(115688784, "e9e25ce047f544033644ecbe32dba26554eb25f98c92ca5abe48d53e107fe034") // prior to erroneous swap cal ETH0.5, https://cardanoscan.io/transaction/a988fb387810671ed56d9b9a286b84ea31b3c15033129596725f70cba155a558?tab=utxo
        //val point = Point(115667964,"afdb94fa19acb040eab85619273d2ebadcc81a2b24b0d69426fc2e8f8b901938") // prior to buggy data
        //val point = Point(112587104, "3f5985f4172fbcff021d47ca55c8adec2572425205ee1c70183a20809c7f5e96") // prior to where is a tx with a duel op; buy and sell op (i.e. tx: 3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e)
        //val point = Point(112587070, "3763de0e50e1c86f929581642254aac30074225b1bd05aa4f3ed80d99647cab3") // prior to where there is a normal single swap
        //val point = Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b") // prior to a point where there is a single swap
        val startPoint = Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b") // prior to a point where there is a single swap

        /* ALL DEXES */
        //val dexes = listOf(0,1,2)
        //val outputFile = "src/test/resources/testdata/transactions_swaps_from_block_${startPoint.slot}.json"

        /* WINGRIDERS */
//        val dexes = listOf(0)
//        val outputFile = "src/test/resources/testdata/wr/transactions_qualified_from_block_${startPoint.slot}.json"

        /* SUNDAESWAP */
//        val dexes = listOf(1)
//        val outputFile = "src/test/resources/testdata/ss/transactions_qualified_from_block_${startPoint.slot}.json"

        /* MINSWAP */
        val dexes = listOf(2)
        val outputFile = "src/test/resources/testdata/minswap/transactions_qualified_from_block_${startPoint.slot}.json"

        val writer: PrintWriter = File(outputFile).printWriter()
        val allTxDTOs = mutableListOf<FullyQualifiedTxDTO>()
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            log.info("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")
            val qualifiedTx = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
            val filteredDexSpecificTx = qualifiedTx
                .filter { dexes.contains(it.dexCode) } // SPECIFIC DEXES
            allTxDTOs.addAll(
                filteredDexSpecificTx
            )
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
//        val outputFile = "src/test/resources/testdata/wr/transactions_qualified_from_block_9894905.json"

//        /* SUNDAESWAP */
//        val dexes = listOf(1)
//        val inputFile = "src/test/resources/testdata/blocks_from_9894905_10tx.json"
//        val outputFile = "src/test/resources/testdata/ss/transactions_qualified_from_block_9894905.json"

        /* MINSWAP */
        val dexes = listOf(2)
        val inputFile = "src/test/resources/testdata/blocks_from_9894905_10tx.json"
        val outputFile = "src/test/resources/testdata/minswap/transactions_qualified_from_block_9894905.json"

        val writer: PrintWriter = File(outputFile).printWriter()
        val blocks: List<Block> = transactionBodyGson.fromJson<List<Block>>(
            File(inputFile).readText(Charsets.UTF_8).byteInputStream().bufferedReader().readLine(),
            object : TypeToken<ArrayList<Block>>() {}.type)
        val qualifiedTx: List<FullyQualifiedTxDTO> = blocks.flatMap {
            chainService.qualifyTransactions(it.header.headerBody.slot, it.transactionBodies, it.transactionWitness)
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
//        val outputFile = "src/test/resources/testdata/wr/swaps_from_block_9894905.json"
//        val inputFile = "src/test/resources/testdata/wr/transactions_qualified_from_block_9894905.json"
//        val outputFile = "src/test/resources/testdata/wr/swaps_from_block_112501875_500tx.json"
//        val inputFile = "src/test/resources/testdata/wr/transactions_qualified_from_block_112501875_500tx.json"

//        /* SUNDAESWAP */
//        val outputFile = "src/test/resources/testdata/ss/swaps_from_block_112501875_500tx.json"
//        val inputFile = "src/test/resources/testdata/ss/transactions_qualified_from_block_112501875_500tx.json"

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