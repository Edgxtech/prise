package tech.edgx.prise.indexer.service.classifier

import com.bloxbean.cardano.yaci.core.common.Constants
import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.model.Era
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.helper.BlockSync
import com.bloxbean.cardano.yaci.helper.listener.BlockChainDataListener
import com.bloxbean.cardano.yaci.helper.model.Transaction
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
import tech.edgx.prise.indexer.util.Helpers

import java.io.File
import java.io.PrintWriter
import java.math.BigInteger
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MinswapClassifierIT: BaseWithCarp() {

    val chainService: ChainService by inject { parametersOf(config) }
    val minswapClassifier: DexClassifier by inject(named("minswapClassifier"))

    val point_01Jan24 = Point(112500883, "d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8")
    val slot_01Jan24 = 112500909L
    val slot_01Jan24_0100 = 112504509L
    val slot_01Jan24_0005 = 112501209L
    val slot_01Jan24_0010 = 112501509L
    val slot_01Jan24_0020 = 112502109L

    @Test
    fun computeSwaps_SingleTransaction_1() {
        /*
            RJV - ADA, Sell, 0.041 ADA, 11,341.315232 RJV, 465.245143 ADA, addr...mddj, 2024-01-01 19:21 GMT+8
            TX: 79a56a719258ca13715bb82c24c869590c37859bba8329667a52f414950c13d1, 112541775, 9748371
            EXPECTING: "79a56a719258ca13715bb82c24c869590c37859bba8329667a52f414950c13d1"	112541775	2	"lovelace"	"8cfd6893f5f6c1cc954cec1a0a1460841b74da6e7803820dde62bb78524a56"	465245143	11341315232	1
        */
        val knownSwaps = listOf<Swap>(
            Swap("79a56a719258ca13715bb82c24c869590c37859bba8329667a52f414950c13d1", 112541775, 2, "lovelace", "8cfd6893f5f6c1cc954cec1a0a1460841b74da6e7803820dde62bb78524a56", BigInteger.valueOf(465245143), BigInteger.valueOf(11341315232), 1),
        )
        val startPoint = Point(112541716, "ca96dbfd879fae1fa48717fabe3e0d313460c3e574247b6f1f58cc8f073e4036")
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = chainService.qualifyTransactions(
                        block.header.headerBody.slot,
                        block.transactionBodies,
                        block.transactionWitness
                    )

                    /* Compute swaps and add/update assets and latest prices */
                    qualifiedTxMap.forEach txloop@{ txDTO ->
                        println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                        val swaps = minswapClassifier.computeSwaps(txDTO)
                        if (swaps.isNotEmpty()) {
                            allSwaps.addAll(swaps)
                        }
                    }
                    val v1Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAP.code }
                    assertTrue(v1Swaps.isNotEmpty())
                    assertEquals(knownSwaps.size, v1Swaps.size)
                    v1Swaps.zip(knownSwaps).forEach {
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
                    running = false
                }
            }
        )
        runBlocking {
            while(running) { delay(100) }
            blockSync.stop()
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_2() {
        /*
            SNEK - ADA, Buy 0.00255 ADA, 1,789.513117 ADA, 701,549 SNEK, addr...mddj 2024-01-01 22:36 GMT+8
            TX: b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d, 112553479, 9748903
            EXPECTING: "b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d"	112553479	2	"lovelace"	"279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b"	1789013117	701549	0
        */
        val knownSwaps = listOf<Swap>(
            Swap("b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d", 112553479, 2, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigInteger.valueOf(1789013117), BigInteger.valueOf(701549), DexOperationEnum.SELL.code),
        )
        val startPoint = Point(112553452, "d2c5b21453a68bd91fb4251fbf420fd0affba21374cdc0d2a4177ea76e9a4844")
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)

                    /* Compute swaps and add/update assets and latest prices */
                    qualifiedTxMap.forEach txloop@{ txDTO -> //dexMatched,
                        println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                        val swaps = minswapClassifier.computeSwaps(txDTO)
                        if (swaps.isNotEmpty()) {
                            allSwaps.addAll(swaps)
                        }
                    }
                    val v1Swaps = allSwaps
                        .filter { it.dex==DexEnum.MINSWAP.code }
                        .filter { swap -> knownSwaps.map { it.txHash}.contains(swap.txHash) }
                    assertTrue(v1Swaps.isNotEmpty())
                    assertEquals(knownSwaps.size, v1Swaps.size)
                    knownSwaps.zip(knownSwaps).forEach {
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
                    running = false
                }
            }
        )
        runBlocking {
            while(running) { delay(100) }
            blockSync.stop()
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_3() {
        /*
            TX: 72875a21809e7c75d0e98e4751171eafc66847dc2614fa70208cfc067d565d90, 112541775, 9748371
            EXPECTING: NOTHING, EVEN THOUGH THERE IS AN OUTPUT TO POOL CONTRACT
        */
        val txHash = "72875a21809e7c75d0e98e4751171eafc66847dc2614fa70208cfc067d565d90"
        val startPoint = Point(116200882, "e8ea5cd88ecc0d98257f49cc4ea8da96936834abb31f90cde4eb6a430e76c1ec")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == 2 }

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = minswapClassifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            val v1Swaps = allSwaps
                .filter { it.dex==DexEnum.MINSWAP.code }
                .filter { swap -> txHash==swap.txHash }
            println("Swaps for specific Tx: $v1Swaps")
            assertTrue(v1Swaps.isEmpty())
            running = false
        }
        runBlocking {
            while(running) { delay(100) }
            subscription.dispose()
            blockStreamer.shutdown()
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_4() {
        /*
            TX: b02042417e8fa4e2386b0e47c85e3f2d18e3483196c861767758ccba52e75730
            HAS no utxos to compute a qty (MinswapV1)
            DOES have a swap for MinswapV2, ignored for this test
            312f82cd4dfae1389e31a07b1b1f23bc849fcb2801e6b671e7c4592438060f61, 116978934
        */
        val txHash = "72875a21809e7c75d0e98e4751171eafc66847dc2614fa70208cfc067d565d90"
        val startPoint = Point(116978934, "312f82cd4dfae1389e31a07b1b1f23bc849fcb2801e6b671e7c4592438060f61")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == DexEnum.MINSWAP.code }
                /* this tx has no outputs for the target asset2 */
                .filter { it.txHash == txHash }
            println("Qualified tx: $qualifiedTxMap")

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = minswapClassifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            println("All swaps: $allSwaps")
            val v1Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAP.code }
            assertTrue(v1Swaps.isEmpty())
            running = false
        }
        runBlocking {
            while(running) { delay(100) }
            subscription.dispose()
            blockStreamer.shutdown()
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_5() {
        // WMT - ADA , Buy , 0.258 ADA , 270 ADA , 1,045.332878 WMT , addr...xuxp , 2024-01-01 08:09 GMT+8
        val txHash = "3cae4bea2849f1cc8546a96058320f0deb949289cbd58295cfc7f50dab71f15a"
        val knownSwaps = listOf<Swap>(
            Swap(txHash, 112501470, DexEnum.MINSWAP.code, "lovelace", "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigInteger.valueOf(270000000), BigInteger.valueOf(1045332878), 0),
        )
        val startPoint = Point(112501437, "03e35373ab1c565ce978bcd8ab17856b79dffdfd3fe6c193f1afa948b3560f3e")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == DexEnum.MINSWAP.code }
                .filter { it.txHash == txHash }
            println("Qualified tx: $qualifiedTxMap")

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = minswapClassifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            println("All swaps: $allSwaps")

            assertEquals(knownSwaps.size, allSwaps.size)
            knownSwaps.zip(allSwaps).forEach {
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
            running = false
        }
        runBlocking {
            while(running) { delay(100) }
            subscription.dispose()
            blockStreamer.shutdown()
        }
    }

    @Test
    fun computeSwaps_FromChainSync_10mins() {
        /*
            TIMESTAMPS:
            01 Jan 24 - 0000
            Epoch timestamp: 1704067200
            Timestamp in seconds: 1704067200
            Date and time (GMT): Monday, 1 January 2024 00:00:00
            SLOT: 1704067200-1591566291=112500909
            Block nearest to slot: 112500909: BlockView(hash=d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8, epoch=458, height=9746375, slot=112500883)

            01 Jan 24: 0010
            Epoch timestamp: 1704067800
            Date and time (GMT): Monday, 1 January 2024 00:10:00
            SLOT:  1704067800-1591566291=112501509
        */
        val fromSlot = slot_01Jan24
        val untilSlot = slot_01Jan24_0100
        val dataFiles = listOf("swaps_wmt_ms_01Jan24","swaps_snek_ms_01Jan24","swaps_rjv_ms_01Jan24")

        val allKnownSwaps = mutableListOf<Swap>()
        dataFiles.forEach {
            val partKnownSwaps: List<Swap> = Gson().fromJson(File("src/test/resources/testdata/minswap/$it.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<Swap?>?>() {}.type)
            allKnownSwaps.addAll(partKnownSwaps)
        }
        val knownSwaps = allKnownSwaps.filter { it.slot in fromSlot..untilSlot }
        val swapAssetUnits = knownSwaps.map { it.asset2Unit }.toSet()

        // start sync from 01 Jan 24 and compute all swaps for the time period
        var allSwaps = mutableListOf<Swap>()
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, point_01Jan24,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO -> //dexMatched,
                if (minswapClassifier.getPoolScriptHash().contains(txDTO.dexCredential)) { // Ignore other dex swaps for this test
                    println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                    val swaps = minswapClassifier.computeSwaps(txDTO)
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

        // Sort them exactly the same way since multiple swaps per tx, and multiple tx per block
        val v1Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAP.code }
        val orderedComputedSwaps = v1Swaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }.filter { it.slot < untilSlot}
            .filter { swapAssetUnits.contains(it.asset2Unit) }
        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }
            .filter { swapAssetUnits.contains(it.asset2Unit) }

        // TEMP, just to speed up devtesting
        val writer: PrintWriter = File("src/test/resources/testdata/minswap/computed_${fromSlot}_${untilSlot}.json").printWriter()
        writer.println(Gson().toJson(orderedComputedSwaps))
        writer.flush()
        writer.close()

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