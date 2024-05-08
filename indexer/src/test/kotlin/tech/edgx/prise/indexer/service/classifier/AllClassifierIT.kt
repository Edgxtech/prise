package tech.edgx.prise.indexer.service.classifier

import com.bloxbean.cardano.yaci.core.common.Constants
import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.model.Era
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.helper.BlockSync
import com.bloxbean.cardano.yaci.helper.listener.BlockChainDataListener
import com.bloxbean.cardano.yaci.helper.model.Transaction
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.test.inject
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.BaseIT
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.service.price.HistoricalPriceHelpers
import tech.edgx.prise.indexer.util.Helpers
import java.math.BigInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AllClassifierIT: BaseIT() {

    val chainService: ChainService by inject { parametersOf(config) }
    val dexClassifiers: List<DexClassifier> by inject(named("dexClassifiers"))
    val sundaeswapClassifier: DexClassifier by inject(named("sundaeswapClassifier"))
    val wingridersClassifier: DexClassifier by inject(named("wingridersClassifier"))
    val minswapClassifier: DexClassifier by inject(named("minswapClassifier"))
    private val chainDatabaseService: ChainDatabaseService by inject(named("carpJDBC")) { parametersOf(config) }

    val point_01Jan24 = Point(112500883, "d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8")
    val slot_01Jan24 = 112500909L
    val slot_01Jan24_0100 = 112504509L

    @Test
    fun computeSwaps_ForOneFifteenCandle() {
        /* Swaps: where slot >= 118544409 and slot < 118545309 */
        val knownSwaps = listOf<Swap>(
            Swap("37e3a61df516df10e982e002a8edb34f30b0e7874a5806bb6466ab23457e0674",118544843L,1,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigInteger.valueOf(3044755254),BigInteger.valueOf(8763634253),1),
            Swap("5614ee00eb985516967d7b6668d73a9534eb6dd1753b97f97a937a9b647dfb1a",118545044L,2,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e",BigInteger.valueOf(1454121764),BigInteger.valueOf(4165587265),0),
            Swap("a4b8224f1926c3663dc7b9df7818ba60c3a0661893df899ad577e9ffacb27520",118545170L,1,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e",BigInteger.valueOf(1900760542),BigInteger.valueOf(5441037479),0)
        )
        val startPoint = Point(118544376, "e1d838c0e6a32bf383c614ee245b1ab21a250810c09757c9699882a21cf0666f")
        val endPointSlot = 118544376 + 900
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
                        //val swaps = minswapClassifier.computeSwaps(txDTO)
                        dexClassifiers.forEach {
                            val swaps = it.computeSwaps(txDTO)
                                .filter { it.asset2Unit=="1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e" }
                            if (swaps.isNotEmpty()) {
                                println("Adding swaps: $swaps")
                                allSwaps.addAll(swaps)
                            }
                        }

                    }
                    if (block.header.headerBody.slot > endPointSlot) {
                        running = false
                    }
                }
            }
        )
        runBlocking {
            while(running) { delay(100) }
            blockSync.stop()
        }
        println("All computed swaps: $allSwaps")
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
    }

    /* Debugging test */
    @Test
    fun computeSwaps_FromChainSync() {
        var allSwaps = mutableListOf<Swap>()
        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        val slot_start = 1679095800 + Helpers.slotConversionOffset
        val slot_end = 1679098500 + Helpers.slotConversionOffset
        val startBlock = chainDatabaseService.getBlockNearestToSlot(slot_start)
        val startPoint = Point(startBlock?.slot!!, startBlock.hash)
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
                        val wrSwaps = wingridersClassifier.computeSwaps(txDTO)
                        println("WR swaps: $wrSwaps")
                        val ssSwaps = sundaeswapClassifier.computeSwaps(txDTO)
                        println("SS swaps: $ssSwaps")
                        val msSwaps = minswapClassifier.computeSwaps(txDTO)
                        println("MS swaps: $msSwaps")
                        allSwaps.addAll(wrSwaps)
                        allSwaps.addAll(ssSwaps)
                        allSwaps.addAll(msSwaps)
                    }
                }
            }
        )
        var runningSwapsCount = 0
        runBlocking {
            while(true) {
                if (allSwaps.isNotEmpty() && allSwaps.size > runningSwapsCount) {
                    println("Running # swaps: ${allSwaps.size}, Up to slot: ${allSwaps.last().slot}, syncing until: $slot_end")
                    runningSwapsCount = allSwaps.size
                }

                if (allSwaps.isNotEmpty() && allSwaps.last().slot > slot_end) {
                    break
                }
                delay(100)
            }
            blockSync.stop()
        }

        val tgtSymbol = "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e"
        val allTgtSwaps = allSwaps.filter { it.asset2Unit==tgtSymbol}
        println("All Tgt swaps $slot_start to $slot_end: $allTgtSwaps")
        val fromAsset = Asset.invoke {
            unit = tgtSymbol
            native_name = "<tgtname>"
            decimals = 6
        }
        val toAsset = Helpers.adaAsset
        val convertedPrices = HistoricalPriceHelpers.transformTradesToPrices(allTgtSwaps, fromAsset, toAsset)
        println("Converted prices (WMT): $convertedPrices")
    }

    /* Debugging test */
    @Test
    fun computeSwaps_FromChainSync_2() {
        var allSwaps = mutableListOf<Swap>()
        val blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        val slot_start = 1689626700 + Helpers.slotConversionOffset
        val slot_end = 1689628500 + Helpers.slotConversionOffset
        val startBlock = chainDatabaseService.getBlockNearestToSlot(slot_start)
        val startPoint = Point(startBlock?.slot!!, startBlock.hash)
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
                        val wrSwaps = wingridersClassifier.computeSwaps(txDTO)
                        println("WR swaps: $wrSwaps")
                        val ssSwaps = sundaeswapClassifier.computeSwaps(txDTO)
                        println("SS swaps: $ssSwaps")
                        val msSwaps = minswapClassifier.computeSwaps(txDTO)
                        println("MS swaps: $msSwaps")
                        allSwaps.addAll(wrSwaps)
                        allSwaps.addAll(ssSwaps)
                        allSwaps.addAll(msSwaps)
                    }
                }
            }
        )
        var runningSwapsCount = 0
        runBlocking {
            while(true) {
                if (allSwaps.isNotEmpty() && allSwaps.size > runningSwapsCount) {
                    println("Running # swaps: ${allSwaps.size}, Up to slot: ${allSwaps.last().slot}, syncing until: $slot_end")
                    runningSwapsCount = allSwaps.size
                }

                if (allSwaps.isNotEmpty() && allSwaps.last().slot > slot_end) {
                    break
                }
                delay(100)
            }
            blockSync.stop()
        }

        val tgtSymbol = "a2944573e99d2ed3055b808eaa264f0bf119e01fc6b18863067c63e44d454c44"
        val allTgtSwaps = allSwaps.filter { it.asset2Unit==tgtSymbol}
        println("All Tgt swaps $slot_start to $slot_end: $allTgtSwaps")
        val fromAsset = Asset.invoke {
            unit = tgtSymbol
            native_name = "<tgtname>"
            decimals = 6
        }
        val toAsset = Helpers.adaAsset
        val convertedPrices = HistoricalPriceHelpers.transformTradesToPrices(allTgtSwaps, fromAsset, toAsset)
        println("Converted prices: $convertedPrices")
    }
}