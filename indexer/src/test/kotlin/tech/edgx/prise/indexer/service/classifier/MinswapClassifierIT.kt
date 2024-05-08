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
import tech.edgx.prise.indexer.service.BaseIT
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.util.Helpers

import java.io.File
import java.io.PrintWriter
import java.math.BigInteger
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MinswapClassifierIT: BaseIT() {

    val chainService: ChainService by inject { parametersOf(config) }
    val minswapClassifier: DexClassifier by inject(named("minswapClassifier"))

    val point_01Jan24 = Point(112500883, "d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8")
    val slot_01Jan24 = 112500909L
    val slot_01Jan24_0100 = 112504509L

    @Test
    fun computeSwaps_SingleTransaction_1() {
        /*
            TX: 79a56a719258ca13715bb82c24c869590c37859bba8329667a52f414950c13d1, 112541775, 9748371
            EXPECTING:
                "79a56a719258ca13715bb82c24c869590c37859bba8329667a52f414950c13d1"	112541775	2	"lovelace"	"8cfd6893f5f6c1cc954cec1a0a1460841b74da6e7803820dde62bb78524a56"	465245143	11341315232	1
            Point(112541716, "ca96dbfd879fae1fa48717fabe3e0d313460c3e574247b6f1f58cc8f073e4036")
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
            TX: b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d, 112553479, 9748903
            EXPECTING:
                "b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d"	112553479	2	"lovelace"	"279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b"	1789013117	701549	0
            StartPoint: Point(112553452, "d2c5b21453a68bd91fb4251fbf420fd0affba21374cdc0d2a4177ea76e9a4844")
        */
        val knownSwaps = listOf<Swap>(
            Swap("b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d", 112553479, 2, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigInteger.valueOf(1789013117), BigInteger.valueOf(701549), 0),
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
                    val v1Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAP.code }
                    val v2Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAPV2.code }
                    assertTrue(v1Swaps.isNotEmpty())
                    assertTrue(v2Swaps.isNotEmpty())
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
    fun computeSwaps_SingleTransaction_3() {
        /*
            TX: 72875a21809e7c75d0e98e4751171eafc66847dc2614fa70208cfc067d565d90, 112541775, 9748371
            EXPECTING: NOTHING, EVEN THOUGH THERE IS AN OUTPUT TO LP (MinswapV2)
                       MinswapV2 swaps ignored for this test
            Point(116200882, "ca96dbfd879fae1fa48717fabe3e0d313460c3e574247b6f1f58cc8f073e4036")
        */
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
            val v1Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAP.code }
            val v2Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAPV2.code }
            assertTrue(v1Swaps.isEmpty())
            assertTrue(v2Swaps.isNotEmpty())
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
        val startPoint = Point(116978934, "312f82cd4dfae1389e31a07b1b1f23bc849fcb2801e6b671e7c4592438060f61")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
                .filter { it.dexCode == 2 }
                /* this tx has no outputs for the target asset2 */
                .filter { it.txHash == "b02042417e8fa4e2386b0e47c85e3f2d18e3483196c861767758ccba52e75730" }
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
            val v2Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAPV2.code }
            assertTrue(v1Swaps.isEmpty())
            assertTrue(v2Swaps.isNotEmpty())
            running = false
        }
        runBlocking {
            while(running) { delay(100) }
            subscription.dispose()
            blockStreamer.shutdown()
        }
    }

    @Test
    fun computeSwaps_FromChainSync_1HR() {
        /*
            TIMESTAMPS:
            01 Jan 24 -
            Epoch timestamp: 1704067200
            Timestamp in seconds: 1704067200
            Date and time (GMT): Monday, 1 January 2024 00:00:00
            SLOT: 1704067200-1591566291=112500909
            Block nearest to slot: 112500909: BlockView(hash=d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8, epoch=458, height=9746375, slot=112500883)

            01 Jan 24: 0100
            Epoch timestamp: 1704070800
            Timestamp in milliseconds: 1704070800000
            Date and time (GMT): Monday, 1 January 2024 01:00:00
            Date and time (your time zone): Monday, 1 January 2024 09:00:00 GMT+08:00
            SLOT:  1704070800-1591566291=112504509
        */
        val reader = File("src/test/resources/testdata/minswap/swaps_0000Z01Jan24_0100Z01Jan24.csv")
            .readText(Charsets.UTF_8).byteInputStream().bufferedReader()
        reader.readLine()
        val knownSwaps: List<Swap> = reader.lineSequence()
            .filter { it.isNotBlank() }
            .map {
                val parts = it.split(",")
                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigInteger(), parts[6].toBigInteger(), parts[7].toInt() )
            }
            .filter { it.slot in slot_01Jan24..slot_01Jan24_0100 } // Filter to 01 Jan 24 00:00 to 01:00 HOUR only
            .toList()
        println("Last known swap: ${knownSwaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(knownSwaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")

        // start sync from 01 Jan 24 and compute all swaps for one month
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
                    println("Running # swaps: ${allSwaps.size}, Up to slot: ${allSwaps.last().slot}, syncing until: $slot_01Jan24_0100")
                    runningSwapsCount = allSwaps.size
                }

                if (allSwaps.isNotEmpty() && allSwaps.last().slot > slot_01Jan24_0100) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }

        // Sort them exactly the same way since multiple swaps per tx, and multiple tx per block
        val v1Swaps = allSwaps.filter { it.dex==DexEnum.MINSWAP.code }
        val orderedComputedSwaps = v1Swaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }.filter { it.slot < slot_01Jan24_0100 }
        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }

        // TEMP, just to speed up devtesting
        val writer: PrintWriter = File("src/test/resources/testdata/minswap/computed_swaps_0000Z01Jan24-0100Z01Jan24.json").printWriter()
        writer.println(Gson().toJson(orderedComputedSwaps))
        writer.flush()
        writer.close()

        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        assertEquals(orderedComputedSwaps.size, orderedKnownSwaps.size)
        orderedComputedSwaps.zip(orderedKnownSwaps).forEach {
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
    fun computeSwaps_FromSaved_1HR() {
        val reader = File("src/test/resources/testdata/minswap/swaps_0000Z01Jan24_0100Z01Jan24.csv")
            .readText(Charsets.UTF_8).byteInputStream().bufferedReader()
        reader.readLine()
        val knownSwaps: List<Swap> = reader.lineSequence()
            .filter { it.isNotBlank() }
            .map {
                val parts = it.split(",")
                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigInteger(), parts[6].toBigInteger(), parts[7].toInt() )
            }
            .filter { it.slot in slot_01Jan24..slot_01Jan24_0100 } // Filter to 01 Jan 24 00:00 to 01:00 HOUR only
            .toList()
        println("Last known swap: ${knownSwaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(knownSwaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")

        // Pull previously computed swaps, just for efficiency it takes a while to compute
        val orderedComputedSwaps: List<Swap> = Gson().fromJson(
            File("src/test/resources/testdata/minswap/computed_swaps_0000Z01Jan24-0100Z01Jan24.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<List<Swap>>() {}.type)

        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }
        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        var idx = 0
        assertEquals(orderedComputedSwaps.size, orderedKnownSwaps.size)
        orderedComputedSwaps.zip(orderedKnownSwaps).forEach {
            println("Comparing computed swap, idx: $idx, ${it.first} vs known: ${it.second}, ")
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.operation == it.second.operation }
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.dex== it.second.dex }
            idx++
        }
    }
}