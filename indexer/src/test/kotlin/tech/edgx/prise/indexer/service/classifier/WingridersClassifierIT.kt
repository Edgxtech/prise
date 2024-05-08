package tech.edgx.prise.indexer.service.classifier

import com.bloxbean.cardano.client.plutus.spec.*
import com.bloxbean.cardano.yaci.core.common.Constants
import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.*
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.helper.BlockSync
import com.bloxbean.cardano.yaci.helper.listener.BlockChainDataListener
import com.bloxbean.cardano.yaci.helper.model.Transaction
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.core.component.get
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.test.inject
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.BaseIT
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.price.HistoricalPriceHelpers
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.io.PrintWriter
import java.math.BigInteger
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WingridersClassifierIT: BaseIT() {

    val chainService: ChainService by inject { parametersOf(config) }
    val wingridersClassifier: DexClassifier by inject(named("wingridersClassifier"))

    val point_01Jan24 = Point(112500883, "d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8")
    val slot_01Jan24 = 112500909L // 112500909
    val slot_02Jan24 = 112587309L
    val slot_01Jan24_0100 = 112504509L

    @Test
    fun computeSwaps_SingleTransaction_1() {
        /*
            TX: 8f0de85877bcafe95ba53c9c552aaa1e3f61a3418cbd9ce2793d541a32b77ede, 117079522, 9967826
            EXPECTING:
            2274.680239 ADA -> 1362.368396 DJED    ==   amount1=2274680239, amount2=1362368396, operation=0(sell)
            3298.577564 DJED -> 5493.578611 ADA    ==   amount1=5493578611, amount2=3298577564, operation=1(buy)
            Point(117079494, "9a9a9285ed3bf3ad72dabee6950413e5518b5661a2c0fa20bfbd029f8e365020")
        */
        val knownSwaps = listOf(
            Swap("8f0de85877bcafe95ba53c9c552aaa1e3f61a3418cbd9ce2793d541a32b77ede", 117079522, 0, "lovelace", "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344", BigInteger.valueOf(5493578611), BigInteger.valueOf(3298577564), 1),
            Swap("8f0de85877bcafe95ba53c9c552aaa1e3f61a3418cbd9ce2793d541a32b77ede", 117079522, 0, "lovelace", "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344", BigInteger.valueOf(2274680239), BigInteger.valueOf(1362368396), 0)
        )
        val startPoint = Point(117079494, "9a9a9285ed3bf3ad72dabee6950413e5518b5661a2c0fa20bfbd029f8e365020")
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
                        val swaps = wingridersClassifier.computeSwaps(txDTO)
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
            TX: ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab
            EXPECTING:
            "ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab"	112501933	0	"lovelace"	"51a5e236c4de3af2b8020442e2a26f454fda3b04cb621c1294a0ef34424f4f4b"	252819173	5000000000	1
            Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b")
        */
        val knownSwaps = listOf(
            Swap("ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab", 112501933, 0, "lovelace", "51a5e236c4de3af2b8020442e2a26f454fda3b04cb621c1294a0ef34424f4f4b", BigInteger.valueOf(252819173), BigInteger.valueOf(5000000000), 1),
        )
        val startPoint = Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b")
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
                        val swaps = wingridersClassifier.computeSwaps(txDTO)
                        println("COMPUTED SWAP: $swaps")
                        if (swaps.isNotEmpty()) {
                            allSwaps.addAll(swaps)
                        }
                    }
                    /* This particular block has 4 separate transactions with swaps, just want to compare single swap for this test */
                    val specificComputedSwaps =
                        allSwaps.filter { it.txHash == "ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab" }
                    assertEquals(specificComputedSwaps.size, knownSwaps.size)
                    specificComputedSwaps.zip(knownSwaps).forEach {
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
            TX: "txHash":"3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e","blockSlot":112587121, height: 9750573
            EXPECTING, 2 swaps:
            "txhash"	"slot"	"dex"	"asset1unit"	"asset2unit"	"amount1"	"amount2"	"operation"
            "3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e"	112587121	0	"lovelace"	"b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d"	5391695121	35000000000	1      buy
            "3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e"	112587121	0	"lovelace"	"b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d"	4050000000	26169677478	0      sell
        */
        val knownSwaps = listOf(
            Swap("3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e", 112587121, 0, "lovelace", "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d", BigInteger.valueOf(5391695121), BigInteger.valueOf(35000000000), 1),
            Swap("3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e", 112587121, 0, "lovelace", "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d", BigInteger.valueOf(4050000000), BigInteger.valueOf(26169677478), 0),
        )
        val startPoint = Point(112587104, "3f5985f4172fbcff021d47ca55c8adec2572425205ee1c70183a20809c7f5e96")
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
                        val swaps = wingridersClassifier.computeSwaps(txDTO)
                        println("COMPUTED SWAP: $swaps")
                        if (swaps.isNotEmpty()) {
                            allSwaps.addAll(swaps)
                        }
                    }
                    /* This particular block has 4 seperate transactions with swaps, just want to compare single swap for this test */
                    //val specificComputedSwaps = allSwaps.filter { it.txHash=="ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab" }
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

    /*
        LOW fee example, post the change in fees
        https://cardanoscan.io/transaction/0de4bd877c07a36e34cad1d21e04e9c66d89d88b5215bd757102af03de6aab9e
        SWAP ADA/WRT 100 ADA, 1 260.974347 WRT   (should be 101.150000 ADA)?
    */
    @Test
    fun computeSwaps_SingleTransaction_4() {
        val knownSwaps = listOf(
            Swap("0de4bd877c07a36e34cad1d21e04e9c66d89d88b5215bd757102af03de6aab9e", 118806827, 0, "lovelace", "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d507357696e67526964657273", BigInteger.valueOf(100000000), BigInteger.valueOf(1260974347), 0),
        )
        val startPoint = Point(118806802, "f91f872bea632996bedae5c5766298eb1475a1dd0809750b5aba8501a64621c2")
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
                        val swaps = wingridersClassifier.computeSwaps(txDTO)
                        println("COMPUTED SWAP: $swaps")
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

    /*
        MID fee example, post the change in fees
        https://cardanoscan.io/transaction/044f918a95b9b93c75cc337d42668156322378f921c486824b2d914780718604
        SWAP ADA/WRT 276.880431 ADA, 3474.683404 WRT (should be 277.380431 ADA)?
    */
    @Test
    fun computeSwaps_SingleTransaction_5() {
        val knownSwaps = listOf(
            Swap("044f918a95b9b93c75cc337d42668156322378f921c486824b2d914780718604", 118794977, 0, "lovelace", "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d507357696e67526964657273", BigInteger.valueOf(276880431), BigInteger.valueOf(3474683404), 0),
        )
        val startPoint = Point(118794900, "7678fd9b61c3e3dbefc357df6e88cf30a07a1562d7917b7ce18e90c1777be49d")
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
                        val swaps = wingridersClassifier.computeSwaps(txDTO)
                        println("COMPUTED SWAP: $swaps")
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

            02 Jan 24
            Epoch timestamp: 1704153600
            Timestamp in milliseconds: 1704153600000
            Date and time (GMT): Tuesday, 2 January 2024 00:00:00
            Date and time (your time zone): Tuesday, 2 January 2024 08:00:00 GMT+08:00
            SLOT: 1704153600-1591566291=112587309

            01 Feb 24 -
            Epoch timestamp: 1706745600
            Timestamp in seconds: 1706745600
            Date and time (GMT): Thursday, 1 February 2024 00:00:00
            1706745600-1591566291=115179309
            Block nearest to slot: 115179309: BlockView(hash=9c84e3bcf7695475173b911a9922271058c9b728f70fc1d427f47a081191d68d, epoch=464, height=9875474, slot=115179294)
        */
        val reader = File("src/test/resources/testdata/wr/swaps_0000Z01Jan24_0100Z01Jan24.csv")
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
//        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, point_01Jan24,  NetworkType.MAINNET.n2NVersionTable)
//        val blockFlux = blockStreamer.stream()
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
                        if (wingridersClassifier.getPoolScriptHash()
                                .contains(txDTO.dexCredential)
                        ) { // Ignore other dex swaps for this test
                            println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                            val swaps = wingridersClassifier.computeSwaps(txDTO)
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
        val writer: PrintWriter = File("src/test/resources/testdata/wr/computed_swaps_0000Z01Jan24-0100Z01Jan24.json").printWriter()
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
        val reader = File("src/test/resources/testdata/wr/swaps_0000Z01Jan24_0100Z01Jan24.csv")
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
        File("src/test/resources/testdata/wr/computed_swaps_0000Z01Jan24-0100Z01Jan24.json")
            .readText(Charsets.UTF_8)
            .byteInputStream()
            .bufferedReader().readLine(),
        object : TypeToken<List<Swap>>() {}.type)

        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }
        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
        var idx = 0
        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
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