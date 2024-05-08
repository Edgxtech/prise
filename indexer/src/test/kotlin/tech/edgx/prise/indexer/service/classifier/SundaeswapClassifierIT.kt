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
class SundaeswapClassifierIT: BaseIT() {

    val chainService: ChainService by inject { parametersOf(config) }
    val sundaeswapClassifier: DexClassifier by inject(named("sundaeswapClassifier"))

    val point_01Jan24 = Point(112500883, "d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8")
    val slot_01Jan24 = 112500909L
    val slot_01Jan24_0100 = 112504509L

    @Test
    fun computeSwaps_SingleTransaction_1() {
        /*
            TX: 589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3, 112502077, 9746431
            EXPECTING:
                "589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3"	112502077	1	"lovelace"	"94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655"	150000000	565416591383	0
                "589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3"	112502077	1	"lovelace"	"94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655"	99021655	338406672751	0
                "589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3"	112502077	1	"lovelace"	"94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655"	165772912	585170875225	1
            Point(112501957, "b1ce9d6f21e19d9eb3b9872f0561bdc044d2b38f8f527b98fe641e3cdc1f4347")
        */
        val knownSwaps = listOf<Swap>(
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(150000000), BigInteger.valueOf(565416591383), 0),
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(99021655), BigInteger.valueOf(338406672751), 0),
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(165772912), BigInteger.valueOf(585170875225), 1)
        )
        val startPoint = Point(112501957, "b1ce9d6f21e19d9eb3b9872f0561bdc044d2b38f8f527b98fe641e3cdc1f4347")
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)

                    /* Compute swaps and add/update assets and latest prices */
                    qualifiedTxMap.forEach txloop@{ txDTO ->
                        println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                        val swaps = sundaeswapClassifier.computeSwaps(txDTO)
                        if (swaps.isNotEmpty()) {
                            allSwaps.addAll(swaps)
                        }
                    }
                    assertEquals(allSwaps.size, knownSwaps.size)
                    allSwaps.zip(knownSwaps).forEach {
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
            TX: 0dd5f01748068cd6a3a65273159447bf38a9c78de04f28e4f8ad0465d384af6c, 112504002, 9746533
            EXPECTING:
                "0dd5f01748068cd6a3a65273159447bf38a9c78de04f28e4f8ad0465d384af6c"	112504002	1	"lovelace"	"682fe60c9918842b3323c43b5144bc3d52a23bd2fb81345560d73f634e45574d"	30556170	2056039876	0
            Point(112503994, "77a240d6353137086cc8a191a30502f85cc72c7d29e4caa5f2aa9cfffb04e304")
        */
        val knownSwaps = listOf<Swap>(
            Swap("0dd5f01748068cd6a3a65273159447bf38a9c78de04f28e4f8ad0465d384af6c", 112504002, 1, "lovelace", "682fe60c9918842b3323c43b5144bc3d52a23bd2fb81345560d73f634e45574d", BigInteger.valueOf(30556170), BigInteger.valueOf(2056039876), 0),
        )
        val startPoint = Point(112503994, "77a240d6353137086cc8a191a30502f85cc72c7d29e4caa5f2aa9cfffb04e304")
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
                    qualifiedTxMap.forEach txloop@{ txDTO -> //dexMatched,
                        println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                        val swaps = sundaeswapClassifier.computeSwaps(txDTO)
                        if (swaps.isNotEmpty()) {
                            allSwaps.addAll(swaps)
                        }
                    }
                    assertEquals(allSwaps.size, knownSwaps.size)
                    allSwaps.zip(knownSwaps).forEach {
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
            TX: 37e3a61df516df10e982e002a8edb34f30b0e7874a5806bb6466ab23457e0674, 118544843L
        */
        val knownSwaps = listOf<Swap>(
            //Swap("37e3a61df516df10e982e002a8edb34f30b0e7874a5806bb6466ab23457e0674",118544843L,1,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e",BigInteger.valueOf(2165579416847),BigInteger.valueOf(6223185439580),2),
            Swap("37e3a61df516df10e982e002a8edb34f30b0e7874a5806bb6466ab23457e0674",118544843L,1,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigInteger.valueOf(3044755254),BigInteger.valueOf(8763634253),1),
        )
        val startPoint = Point(118544837, "adc25a9c4a316079e810daa7d7aad46b52721beac77b57c09593c213bfb971c4")
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
                    qualifiedTxMap.forEach txloop@{ txDTO -> //dexMatched,
                        println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                        val swaps = sundaeswapClassifier.computeSwaps(txDTO)
                        if (swaps.isNotEmpty()) {
                            println("Adding swaps: $swaps")
                            allSwaps.addAll(swaps)
                        }
                    }
                    assertEquals(knownSwaps.size, allSwaps.size)
                    allSwaps.zip(knownSwaps).forEach {
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
            while(running) { delay(1) }
            blockSync.stop()
        }
    }

    @Test
    fun computeSwaps_SingleTransaction_4() {
        /*
            TX: bc0f97141776e473701a1007585861bb70ff73308c8886446416c0996153a33c, 87531387
        */
        val knownSwaps = listOf<Swap>(
            Swap("bc0f97141776e473701a1007585861bb70ff73308c8886446416c0996153a33c",87531387L,1,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e",BigInteger.valueOf(978748416),BigInteger.valueOf(935712864),1),
        )
        val startPoint = Point(87531365, "5a308590421f2d55667fd537b4c8bfcd4538adb595606352ef6be633485bed71")
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
                        if (txDTO.dexCode==DexEnum.SUNDAESWAP.code && txDTO.txHash=="bc0f97141776e473701a1007585861bb70ff73308c8886446416c0996153a33c") {
                            println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                            val swaps = sundaeswapClassifier.computeSwaps(txDTO)
                            if (swaps.isNotEmpty()) {
                                println("Adding swaps: $swaps")
                                allSwaps.addAll(swaps)
                            }
                        }
                    }
                    assertEquals(knownSwaps.size, allSwaps.size)
                    allSwaps.zip(knownSwaps).forEach {
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
            while(running) { delay(1) }
            blockSync.stop()
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
        val reader = File("src/test/resources/testdata/ss/swaps_0000Z01Jan24_0100Z01Jan24.csv")
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
        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(point_01Jan24,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = chainService.qualifyTransactions(
                        block.header.headerBody.slot,
                        block.transactionBodies,
                        block.transactionWitness
                    )

                    /* Compute swaps and add/update assets and latest prices */
                    qualifiedTxMap.forEach txloop@{ txDTO -> //dexMatched,
                        if (sundaeswapClassifier.getPoolScriptHash()
                                .contains(txDTO.dexCredential)
                        ) { // Ignore other dex swaps for this test
                            println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                            val swaps = sundaeswapClassifier.computeSwaps(txDTO)
                            if (swaps.isNotEmpty()) {
                                allSwaps.addAll(swaps)
                            }
                        }
                    }
                }
            }
        )
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
            blockSync.stop()
        }

        // Sort them exactly the same way since multiple swaps per tx, and multiple tx per block
        val orderedComputedSwaps = allSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }.filter { it.slot < slot_01Jan24_0100 }
        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }

        // TEMP, just to speed up devtesting
        val writer: PrintWriter = File("src/test/resources/testdata/ss/computed_swaps_0000Z01Jan24-0100Z01Jan24.json").printWriter()
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
        val reader = File("src/test/resources/testdata/ss/swaps_0000Z01Jan24_0100Z01Jan24.csv")
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
            File("src/test/resources/testdata/ss/computed_swaps_0000Z01Jan24-0100Z01Jan24.json")
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

    @Test
    fun computeSwaps_SingleTransaction_1_Reactive() {
        /*
            TX: 589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3, 112502077, 9746431
            EXPECTING:
                "589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3"	112502077	1	"lovelace"	"94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655"	150000000	565416591383	0
                "589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3"	112502077	1	"lovelace"	"94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655"	99021655	338406672751	0
                "589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3"	112502077	1	"lovelace"	"94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655"	165772912	585170875225	1
            Point(112501957, "b1ce9d6f21e19d9eb3b9872f0561bdc044d2b38f8f527b98fe641e3cdc1f4347")
        */
        val knownSwaps = listOf<Swap>(
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(150000000), BigInteger.valueOf(565416591383), 0),
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(99021655), BigInteger.valueOf(338406672751), 0),
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(165772912), BigInteger.valueOf(585170875225), 1)
        )
        val startPoint = Point(112501957, "b1ce9d6f21e19d9eb3b9872f0561bdc044d2b38f8f527b98fe641e3cdc1f4347")
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val allSwaps = mutableListOf<Swap>()
        var running = true

        val subscription = blockFlux.subscribe { block: Block ->
            println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

            val qualifiedTxMap = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)

            /* Compute swaps and add/update assets and latest prices */
            qualifiedTxMap.forEach txloop@{ txDTO ->
                println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                val swaps = sundaeswapClassifier.computeSwaps(txDTO)
                if (swaps.isNotEmpty()) {
                    allSwaps.addAll(swaps)
                }
            }
            assertEquals(allSwaps.size, knownSwaps.size)
            allSwaps.zip(knownSwaps).forEach {
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
}