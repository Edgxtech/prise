package tech.edgx.prise.indexer.service.classifier

import com.bloxbean.cardano.client.plutus.spec.*
import com.bloxbean.cardano.yaci.core.common.Constants
import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.*
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.helper.BlockSync
import com.bloxbean.cardano.yaci.helper.listener.BlockChainDataListener
import com.bloxbean.cardano.yaci.helper.model.Transaction
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.test.inject
import tech.edgx.prise.indexer.Base
import tech.edgx.prise.indexer.model.dex.SwapDTO
import tech.edgx.prise.indexer.processor.SwapProcessor
import tech.edgx.prise.indexer.service.chain.ChainService
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WingridersClassifierIT: Base() {

    val chainService: ChainService by inject { parametersOf(config) }
    val swapProcessor: SwapProcessor by inject { parametersOf(config) }
    val wingridersClassifier: DexClassifier by inject(named("wingridersClassifier"))

    @Test
    fun computeSwaps_SingleTransaction_1() {
        /*
            TX: 8f0de85877bcafe95ba53c9c552aaa1e3f61a3418cbd9ce2793d541a32b77ede, 117079522, 9967826
            EXPECTING:
            2274.680239 ADA -> 1362.368396 DJED    ==   amount1=2274680239, amount2=1362368396, operation=0(sell)
            3298.577564 DJED -> 5493.578611 ADA    ==   amount1=5493578611, amount2=3298577564, operation=1(buy)
            Point(117079494, "9a9a9285ed3bf3ad72dabee6950413e5518b5661a2c0fa20bfbd029f8e365020")
        */
        val knownSwapDTOS = listOf(
            SwapDTO("8f0de85877bcafe95ba53c9c552aaa1e3f61a3418cbd9ce2793d541a32b77ede", 117079522, 0, "lovelace", "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344", BigDecimal.valueOf(5493578611), BigDecimal.valueOf(3298577564), 1),
            SwapDTO("8f0de85877bcafe95ba53c9c552aaa1e3f61a3418cbd9ce2793d541a32b77ede", 117079522, 0, "lovelace", "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344", BigDecimal.valueOf(2274680239), BigDecimal.valueOf(1362368396), 0)
        )
        val startPoint = Point(117079494, "9a9a9285ed3bf3ad72dabee6950413e5518b5661a2c0fa20bfbd029f8e365020")
        val allSwapDTOS = mutableListOf<SwapDTO>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = swapProcessor.qualifyTransactions(
                        block.header.headerBody.slot,
                        block.transactionBodies,
                        block.transactionWitness
                    )

                    /* Compute swaps and add/update assets and latest prices */
                    qualifiedTxMap.forEach txloop@{ txDTO ->
                        println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
                        val swaps = wingridersClassifier.computeSwaps(txDTO)
                        if (swaps.isNotEmpty()) {
                            allSwapDTOS.addAll(swaps)
                        }
                    }
                    assertEquals(allSwapDTOS.size, knownSwapDTOS.size)
                    allSwapDTOS.zip(knownSwapDTOS).forEach {
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
        val knownSwapDTOS = listOf(
            SwapDTO("ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab", 112501933, 0, "lovelace", "51a5e236c4de3af2b8020442e2a26f454fda3b04cb621c1294a0ef34424f4f4b", BigDecimal.valueOf(252819173), BigDecimal.valueOf(5000000000), 1),
        )
        val startPoint = Point(112501875, "62577f0a70d2451559cc60c6fb2e9f137ef18fc0f3734ee316eaf382e58f678b")
        val allSwapDTOS = mutableListOf<SwapDTO>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = swapProcessor.qualifyTransactions(
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
                            allSwapDTOS.addAll(swaps)
                        }
                    }
                    /* This particular block has 4 separate transactions with swaps, just want to compare single swap for this test */
                    val specificComputedSwaps =
                        allSwapDTOS.filter { it.txHash == "ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab" }
                    assertEquals(specificComputedSwaps.size, knownSwapDTOS.size)
                    specificComputedSwaps.zip(knownSwapDTOS).forEach {
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
        val knownSwapDTOS = listOf(
            SwapDTO("3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e", 112587121, 0, "lovelace", "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d", BigDecimal.valueOf(5391695121), BigDecimal.valueOf(35000000000), 1),
            SwapDTO("3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e", 112587121, 0, "lovelace", "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d", BigDecimal.valueOf(4050000000), BigDecimal.valueOf(26169677478), 0),
        )
        val startPoint = Point(112587104, "3f5985f4172fbcff021d47ca55c8adec2572425205ee1c70183a20809c7f5e96")
        val allSwapDTOS = mutableListOf<SwapDTO>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = swapProcessor.qualifyTransactions(
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
                            allSwapDTOS.addAll(swaps)
                        }
                    }
                    /* This particular block has 4 seperate transactions with swaps, just want to compare single swap for this test */
                    //val specificComputedSwaps = allSwaps.filter { it.txHash=="ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab" }
                    assertEquals(allSwapDTOS.size, knownSwapDTOS.size)
                    allSwapDTOS.zip(knownSwapDTOS).forEach {
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
        val knownSwapDTOS = listOf(
            SwapDTO("0de4bd877c07a36e34cad1d21e04e9c66d89d88b5215bd757102af03de6aab9e", 118806827, 0, "lovelace", "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d507357696e67526964657273", BigDecimal.valueOf(100000000), BigDecimal.valueOf(1260974347), 0),
        )
        val startPoint = Point(118806802, "f91f872bea632996bedae5c5766298eb1475a1dd0809750b5aba8501a64621c2")
        val allSwapDTOS = mutableListOf<SwapDTO>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = swapProcessor.qualifyTransactions(
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
                            allSwapDTOS.addAll(swaps)
                        }
                    }
                    assertEquals(allSwapDTOS.size, knownSwapDTOS.size)
                    allSwapDTOS.zip(knownSwapDTOS).forEach {
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
        val knownSwapDTOS = listOf(
            SwapDTO("044f918a95b9b93c75cc337d42668156322378f921c486824b2d914780718604", 118794977, 0, "lovelace", "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d507357696e67526964657273", BigDecimal.valueOf(276880431), BigDecimal.valueOf(3474683404), 0),
        )
        val startPoint = Point(118794900, "7678fd9b61c3e3dbefc357df6e88cf30a07a1562d7917b7ce18e90c1777be49d")
        val allSwapDTOS = mutableListOf<SwapDTO>()
        var running = true

        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(startPoint,
            object : BlockChainDataListener {
                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")

                    val qualifiedTxMap = swapProcessor.qualifyTransactions(
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
                            allSwapDTOS.addAll(swaps)
                        }
                    }
                    assertEquals(allSwapDTOS.size, knownSwapDTOS.size)
                    allSwapDTOS.zip(knownSwapDTOS).forEach {
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

//    @Test
//    fun computeSwaps_FromChainSync_TimePeriod() {
//        val fromSlot = TestHelpers.slot_01Jan24
//        val untilSlot = TestHelpers.slot_01Jan24_0005
//        val reader = File("src/test/resources/testdata/wingriders/swaps_0000Z01Jan24_0100Z01Jan24.csv")
//            .readText(Charsets.UTF_8).byteInputStream().bufferedReader()
//        reader.readLine()
//        val knownSwaps: List<Swap> = reader.lineSequence()
//            .filter { it.isNotBlank() }
//            .map {
//                val parts = it.split(",")
//                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigDecimal(), parts[6].toBigDecimal(), parts[7].toInt() )
//            }
//            .filter { it.slot in fromSlot..untilSlot } // Filter to 01 Jan 24 00:00 to 01:00 HOUR only
//            .toList()
//        println("Last known swap: ${knownSwaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(knownSwaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")
//
//        // start sync from 01 Jan 24 and compute all swaps for the time period
//        var allSwaps = mutableListOf<Swap>()
//        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
//        blockSync.startSync(
//            TestHelpers.point_01Jan24,
//            object : BlockChainDataListener {
//                override fun onBlock(era: Era, block: Block, transactions: MutableList<Transaction>) {
//                    println("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")
//
//                    val qualifiedTxMap = swapProcessor.qualifyTransactions(
//                        block.header.headerBody.slot,
//                        block.transactionBodies,
//                        block.transactionWitness
//                    )
//
//                    /* Compute swaps and add/update assets and latest prices */
//                    qualifiedTxMap.forEach txloop@{ txDTO -> //dexMatched,
//                        if (wingridersClassifier.getPoolScriptHash()
//                                .contains(txDTO.dexCredential)
//                        ) { // Ignore other dex swaps for this test
//                            println("Computing swaps for ${txDTO.dexCredential}, TX: ${txDTO.txHash}, Dex: ${txDTO.dexCode}")
//                            val swaps = wingridersClassifier.computeSwaps(txDTO)
//                            if (swaps.isNotEmpty()) {
//                                allSwaps.addAll(swaps)
//                            }
//                        }
//                    }
//                }
//            }
//        )
//        var runningSwapsCount = 0
//        runBlocking {
//            while(true) {
//                if (allSwaps.isNotEmpty() && allSwaps.size > runningSwapsCount) {
//                    println("Running # swaps: ${allSwaps.size}, Up to slot: ${allSwaps.last().slot}, syncing until: $untilSlot")
//                    runningSwapsCount = allSwaps.size
//                }
//
//                if (allSwaps.isNotEmpty() && allSwaps.last().slot > untilSlot) {
//                    break
//                }
//                delay(100)
//            }
//            blockSync.stop()
//        }
//
//        // Sort them exactly the same way since multiple swaps per tx, and multiple tx per block
//        val orderedComputedSwaps = allSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }.filter { it.slot < untilSlot }
//        val orderedKnownSwaps = knownSwaps.sortedBy { it.slot }.sortedBy { it.txHash }.sortedBy { it.operation }.sortedBy { it.amount1 }
//
//        println("Comparing known swaps #: ${orderedKnownSwaps.size} to computedSwaps #: ${orderedComputedSwaps.size}")
//        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
//        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
//            println("Comparing: ${it.first} to ${it.second}")
//            assertTrue { it.first.txHash == it.second.txHash }
//            assertTrue { it.first.slot == it.second.slot }
//            assertTrue { it.first.dex == it.second.dex }
//            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
//            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
//            assertTrue { it.first.amount1 == it.second.amount1 }
//            assertTrue { it.first.amount2 == it.second.amount2 }
//            assertTrue { it.first.operation == it.second.operation }
//        }
//    }
}