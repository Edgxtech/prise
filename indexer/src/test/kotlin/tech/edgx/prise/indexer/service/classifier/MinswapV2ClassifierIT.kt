package tech.edgx.prise.indexer.service.classifier

import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.helper.reactive.BlockStreamer
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.test.inject
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.BaseWithCarp
import tech.edgx.prise.indexer.model.DexOperationEnum
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.testutil.TestHelpers
import java.io.File
import java.math.BigInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MinswapV2ClassifierIT: BaseWithCarp() {

    val chainService: ChainService by inject { parametersOf(config) }
    val minswapV2Classifier: DexClassifier by inject(named("minswapV2Classifier"))

    @Test
    fun computeSwaps_SingleTransaction_SingleSwap_1() {
        val swapTxHash = "b5eba3bc2628102ec30d7511cdb3d4a29ba506cbda844671e1ece0dfa896ecfe"
        val knownSwaps = listOf(
            //DEDI/ADA Swap, Buy    0.192 ADA | 200 ADA | 1,041.2254 DEDI addr...0mnq 2024-08-16 08:46 GMT+8
            Swap(swapTxHash, 132202912, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigInteger.valueOf(200000000), BigInteger.valueOf(1041225400), DexOperationEnum.SELL.code),
        )
        val startPoint = Point(132202898, "640f5cbb9a1b0441a6d6c5ad3468fcfac6f950b630f8fae8f8c8d9be63996deb")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        println("Using classifier: ${minswapV2Classifier.getDexName()}")

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == DexEnum.MINSWAPV2.code }
                .filter { it.txHash == swapTxHash }
            println("Qualified tx: $qualifiedTxMap")

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = minswapV2Classifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            running = false
        }
        runBlocking {
            while(running) { delay(100) }
            subscription.dispose()
            blockStreamer.shutdown()
        }

        val orderedComputedSwaps = allSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }
        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_SingleSwap_2() {
        val swapTxHash = "269a8408bb1d47087a164267fcc6488dd65754d31b9c0f1547e63d6850ed35a4"
        val knownSwaps = listOf(
            // DEDI - ADA, Sell, 0.192 ADA, 13,540.519741 DEDI, 2,605.777417 ADA, addr...n73g , 2024-08-16 10:27 GMT+8
            Swap(swapTxHash, 132208958, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigInteger.valueOf(2605777417), BigInteger.valueOf(13540519741), DexOperationEnum.BUY.code),
        )
        val startPoint = Point(132208946, "1ca45de79c39f6c323bf2eecb2a88c0bd92d509c0f98493115d7aaf004103e73")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == DexEnum.MINSWAPV2.code }
                .filter { it.txHash == swapTxHash }

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                val swaps = minswapV2Classifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            running = false
        }
        runBlocking {
            while(running) { delay(100) }
            subscription.dispose()
            blockStreamer.shutdown()
        }

        val orderedComputedSwaps = allSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }
        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_QtySwaps() {
        val swapTxHash = "f01615dc120442a43d08dbcd13c80044aafbde1ddb7c7e8899e3222e3a556858"
        val knownSwaps = listOf(
            Swap(swapTxHash, 132216341, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigInteger.valueOf(2061343905), BigInteger.valueOf(9956660000), DexOperationEnum.BUY.code),
            Swap(swapTxHash, 132216341, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigInteger.valueOf(9862068966), BigInteger.valueOf(48129095766), DexOperationEnum.SELL.code),
        )
        val startPoint = Point(132216322, "ac760a71bf7f2de47147b061f6cf97ed3955240e9323bbb7e6dfbb3a730dc0b4")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == DexEnum.MINSWAPV2.code }
                .filter { it.txHash == swapTxHash }

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = minswapV2Classifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            running = false
        }
        runBlocking {
            while(running) { delay(100) }
            subscription.dispose()
            blockStreamer.shutdown()
        }
        // Sort them exactly the same way since multiple swaps per tx, and multiple tx per block
        val orderedComputedSwaps = allSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }
        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwaps_QtyTransaction_QtySwaps_2() {
        val knownSwaps = listOf(
            Swap("504122667db93a394e78abfabb1f2d40e1205e1a7c14cbdb1b67a63b5e6e57bb", 132291896, DexEnum.MINSWAPV2.code, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigInteger.valueOf(50641844 ), BigInteger.valueOf(23218 ), DexOperationEnum.BUY.code),
            Swap("6b403ef7286497c85edf7ebe55b14877bedac0efbbbda7a5dafbbf077501e606", 132291807, DexEnum.MINSWAPV2.code, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigInteger.valueOf(3056441804 ), BigInteger.valueOf(1400000  ), DexOperationEnum.BUY.code),
            Swap("b73ce1ee2242a09c9b5540f56a9a8e3295f6f95feefb85dbdf7b718890ccbd04", 132291574, DexEnum.MINSWAPV2.code, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigInteger.valueOf(419999450  ), BigInteger.valueOf(188404 ), DexOperationEnum.SELL.code),
            Swap("12c374cf7cb4fac3eba2cfe95579634cf5a18b8d12e503baee9b1123a8b6e899", 132291432, DexEnum.MINSWAPV2.code, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigInteger.valueOf(419999450 ), BigInteger.valueOf(188450 ), DexOperationEnum.SELL.code),
            )
        val swapAssetUnits = knownSwaps.map { it.asset2Unit }.toSet()
        val startPoint = Point(132291412, "920dc423192688d0b30a59d547e85972b62fdf7096dabad75407e2f58e07712b")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        val allQualifiedTxMap = mutableListOf<FullyQualifiedTxDTO>()

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == DexEnum.MINSWAPV2.code }
            println("Qualified tx: $qualifiedTxMap")

            /* temp for re-use in unit tests */
            allQualifiedTxMap.addAll(qualifiedTxMap)

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = minswapV2Classifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            println("All swaps: $allSwaps")
        }
        val untilSlot = knownSwaps.maxOf { it.slot }
        var runningSwapsCount = 0
        runBlocking {
            while(true) {
                if (allSwaps.isNotEmpty() && allSwaps.size > runningSwapsCount) {
                    println("Running # swaps: ${allSwaps.size}, Up to slot: ${allSwaps.last().slot}, syncing until: $untilSlot")
                    runningSwapsCount = allSwaps.size
                }

                if (allSwaps.isNotEmpty() && allSwaps.last().slot > untilSlot) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }

        // Sort them exactly the same way since multiple swaps per tx, and multiple tx per block
        val orderedComputedSwaps = allSwaps
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .filter { it.slot <= untilSlot }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedKnownSwaps = knownSwaps.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_Type4_Deposit() {
        val knownSwaps = listOf(
            /* This gets filtered out since is type==4; DEPOSIT */
            // Orcfax Token, Buy , 0.026 ADA , 42.56296 ADA , 1,631.976871 FACT , addr...xpw0 , 2024-08-16 14:08 GMT+8
            Swap("d3aba39861706b25c5a8c33ce48889be998405a55b646002ee2f085e5a9fcd14", 132222216, DexEnum.MINSWAPV2.code, "lovelace", "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e516f7263666178746f6b656e", BigInteger.valueOf(42562960 ), BigInteger.valueOf(1631976871 ), DexOperationEnum.SELL.code),
        )
        val swapAssetUnits = knownSwaps.map { it.asset2Unit }.toSet()
        val startPoint = Point(132222208,"7e61db002d3b5ad2932b909b3569a76e0c50e907dcdedfb81db1f65a150759b4")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        val allQualifiedTxMap = mutableListOf<FullyQualifiedTxDTO>()

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == DexEnum.MINSWAPV2.code }

            /* temp for re-use in unit tests */
            allQualifiedTxMap.addAll(qualifiedTxMap)

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = minswapV2Classifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
        }
        val untilSlot = knownSwaps.maxOf { it.slot }

        var runningSwapsCount = 0
        runBlocking {
            while(true) {
                if (allSwaps.isNotEmpty() && allSwaps.size > runningSwapsCount) {
                    println("Running # swaps: ${allSwaps.size}, Up to slot: ${allSwaps.last().slot}, syncing until: $untilSlot")
                    runningSwapsCount = allSwaps.size
                }

                if (allSwaps.isNotEmpty() && allSwaps.last().slot > untilSlot) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }

        // Sort them exactly the same way since multiple swaps per tx, and multiple tx per block
        val orderedComputedSwaps = allSwaps
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .filter { it.slot <= untilSlot }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedKnownSwaps = MinswapV2ClassifierTest.filterSwapsByType(knownSwaps, allQualifiedTxMap, listOf(0))
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwaps_FromChainSync_TimePeriod() {
        val fromSlot = TestHelpers.slot_01Aug24
        val untilSlot = TestHelpers.slot_01Aug24_0030

        val knownSwapDataFiles = listOf("swaps_wmt_msv2_01Aug24")
        val allKnownSwaps = knownSwapDataFiles.flatMap {
            val partKnownSwaps: List<Swap> = Gson().fromJson(File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/$it.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
                object : TypeToken<ArrayList<Swap?>?>() {}.type)
            partKnownSwaps
        }
        // start sync from 01 Jan 24 and compute all swaps for the time period
        var allSwaps = mutableListOf<Swap>()
        val allQualifiedTxMap = mutableListOf<FullyQualifiedTxDTO>()
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, TestHelpers.point_01Aug24,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
            allQualifiedTxMap.addAll(qualifiedTxMap)

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO -> //dexMatched,
                if (minswapV2Classifier.getPoolScriptHash().contains(txDTO.dexCredential)) { // Ignore other dex swaps for this test
                    val swaps = minswapV2Classifier.computeSwaps(txDTO)
                    if (swaps.isNotEmpty()) {
                        allSwaps.addAll(swaps)
                    }
                }
            }
        }
        var runningSwapsCount = 0
        runBlocking {
            while(true) {
                if (allSwaps.isNotEmpty() && allSwaps.size > runningSwapsCount) {
                    println("Running # swaps: ${allSwaps.size}, Up to slot: ${allSwaps.last().slot}, syncing until: $untilSlot")
                    runningSwapsCount = allSwaps.size
                }

                if (allSwaps.isNotEmpty() && allSwaps.last().slot > untilSlot) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }

        val orderedKnownSwaps = MinswapV2ClassifierTest.filterSwapsByType(allKnownSwaps, allQualifiedTxMap, listOf(0))
            .filter { it.slot in fromSlot..untilSlot }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val swapAssetUnits = orderedKnownSwaps.map { it.asset2Unit }.toSet()

        val orderedComputedSwaps = allSwaps
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .filter { it.slot < untilSlot}
            .sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }

        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }
}