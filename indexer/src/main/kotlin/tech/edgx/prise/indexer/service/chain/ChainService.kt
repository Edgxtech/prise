package tech.edgx.prise.indexer.service.chain

import com.bloxbean.cardano.yaci.core.common.Constants
import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.model.Era
import com.bloxbean.cardano.yaci.core.model.TransactionBody
import com.bloxbean.cardano.yaci.core.model.Witnesses
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Tip
import com.bloxbean.cardano.yaci.helper.BlockSync
import com.bloxbean.cardano.yaci.helper.listener.BlockChainDataListener
import com.bloxbean.cardano.yaci.helper.model.Transaction
import kotlinx.coroutines.runBlocking
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.CandleService
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.service.price.HistoricalPriceService
import tech.edgx.prise.indexer.service.price.LatestPriceService
import tech.edgx.prise.indexer.thread.KeepAliveThread
import tech.edgx.prise.indexer.util.Helpers
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

class ChainService(private val config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass::class.java)

    private val chainDatabaseService: ChainDatabaseService by inject(named(config.chainDatabaseServiceModule)) { parametersOf(config) }
    val latestPriceService: LatestPriceService by inject { parametersOf(config) }
    val historicalPriceService: HistoricalPriceService by inject { parametersOf(config) }
    val candleService: CandleService by inject { parametersOf(config) }

    lateinit var blockSync: BlockSync

    data class InitialisationState(
        val chainStartPoint: Point,
        val candleStartPoints: Map<Duration,Long>
    )

    /* Inject all DexClassifier modules */
    private val dexClassifiers: List<DexClassifier> by inject(named("dexClassifiers"))

    var initialised = false

    private val dexClassifierMap: Map<String, DexClassifier> = dexClassifiers
        .flatMap {dexClassifier ->
            dexClassifier.getPoolScriptHash().map {
                it to dexClassifier
            } }
        .filter { config.dexClassifiers?.contains(it.second.getDexName())?: false }
        .toMap()

    private val dexPaymentCredentials = dexClassifierMap.keys

    fun startSync(syncStatusCallback: (Long) -> Unit) {
        candleService.addIndexesIfRequired()

        /* determine chain start point && the candle sync start points enabling continuation of candles */
        val initialisationState = determineInitialisationState(config.startPointTime)
        log.info("Using initialisation state: $initialisationState, \nclassifiers: ${dexClassifierMap.values.toSet().map { it.getDexName() }}, \nchain db service: $chainDatabaseService")

        val blockChainDataListener = object : BlockChainDataListener {
            override fun onBlock(era: Era, block: Block, transactions: List<Transaction>) {
                processBlock(block)
                if (block.header.headerBody.blockNumber % 10 == 0L) {
                    syncStatusCallback(block.header.headerBody.slot)
                    log.info("Processed 10 Block(s) until >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, ${block.header.headerBody.slot}")
                }
            }

            override fun onRollback(point: Point?) {
                if (initialised) {
                    log.info("Rollback called, to point: $point, Is initialised: $initialised")
                    if (point?.slot==0L || point?.hash==null) {
                        log.info("Ignoring rollback, no point provided")
                        return
                    }
                    blockSync.stop()

                    /* Re-determine initialisation point, from nearest discrete date IOT buffer swaps
                    *  Use the lower of candleSyncPoint and rollbackPoint for continuity */
                    val rollbackPointTime = point.slot.minus(Helpers.slotConversionOffset)
                    val syncPointTime = candleService.getSyncPointTime()
                    log.info("Candle sync point time: $syncPointTime vs rollbacktime: $rollbackPointTime, is rollback after synch point (SHOULDNT BE): ${(rollbackPointTime > syncPointTime!!)}") //, rollbackInitialisationState: $rollbackInitialisationState, is rollback chain init point after syncPoint (SHOULDNT BE): ${(rollbackInitialisationState.chainStartPoint.slot.minus(Helpers.slotConversionOffset) > syncPointTime!!)}")
                    val reInitialisationTime = when(syncPointTime < rollbackPointTime) {
                        true -> syncPointTime
                        else -> rollbackPointTime
                    }

                    val rollbackInitialisationState = determineInitialisationState(reInitialisationTime)
                    log.info("On rollback, re-initialisation time: $reInitialisationTime, re-initialisation state: $rollbackInitialisationState")

                    /* Pre-set the previous candle states */
                    historicalPriceService.initialiseLastCandleState(rollbackInitialisationState.candleStartPoints)
                    initialised = false
                    blockSync.startSync(rollbackInitialisationState.chainStartPoint, this)
                } else {
                    initialised = true
                    log.info("(Re)Initialised...")
                }
            }

            override fun intersactNotFound(tip: Tip) {
                log.debug("Intersact not found for tip: $tip")
                blockSync.stop()

                /* Re-determine initialisation point, from nearest discrete date IOT buffer swaps */
                val rollbackPointTime = tip.point.slot.minus(Helpers.slotConversionOffset)
                val syncPointTime = candleService.getSyncPointTime()
                log.info("Candle sync point time: $syncPointTime vs rollbacktime: $rollbackPointTime, is rollback after synch point (SHOULDNT BE): ${(rollbackPointTime > syncPointTime!!)}")
                val reInitialisationTime = when(syncPointTime < rollbackPointTime) {
                    true -> syncPointTime
                    else -> rollbackPointTime
                }

                val rollbackInitialisationState = determineInitialisationState(reInitialisationTime)
                log.info("On rollback, re-initialisation time: $reInitialisationTime, re-initialisation state: $rollbackInitialisationState")

                /* Pre-set the previous candle states */
                historicalPriceService.initialiseLastCandleState(rollbackInitialisationState.candleStartPoints)
                blockSync.startSync(rollbackInitialisationState.chainStartPoint, this)
            }
        }

        /* Pre-set the previous candle states */
        historicalPriceService.initialiseLastCandleState(initialisationState.candleStartPoints)

        blockSync = BlockSync(config.cnodeAddress,  config.cnodePort!!, NetworkType.MAINNET.protocolMagic, Constants.WELL_KNOWN_MAINNET_POINT)
        blockSync.startSync(initialisationState.chainStartPoint, blockChainDataListener)
        val keepAliveThread = KeepAliveThread(config, blockSync)
        keepAliveThread.start()
    }

    /* Auto select a point; if no candles, use first dex launch block/slot otherwise check in app
       db for suitable start point to continue where it left off. The requirement here is
       to be able to continue where it left off, which will be the point equal to the smallest
       reso duration table min of each symbol max time */
    fun determineInitialisationState(providedStartTime: Long?): InitialisationState {
        val candleStartPoints = when {
            providedStartTime !== null -> {
                log.info("Starting from or near a provided time: $providedStartTime")
                getDiscreteCandleAdjustedTimes(providedStartTime)
            }
            else -> {
                val syncPointTime = candleService.getSyncPointTime()
                log.info("Retrieved sync point time: $syncPointTime")
                val nearestPoint = when (syncPointTime == null) {
                    true -> {
                        log.info("Starting from known dex launch date")
                        getDiscreteCandleAdjustedTimes(Helpers.dexLaunchTime)
                    }
                    false -> {
                        log.info("Already have some candles, resuming from last sync point: ${syncPointTime}, slot: ${syncPointTime + Helpers.slotConversionOffset}")
                        getDiscreteCandleAdjustedTimes(syncPointTime)
                    }
                }
                nearestPoint
            }
        }
        log.info("Getting block nearest to slot: ${candleStartPoints[Helpers.smallestDuration]?.plus(Helpers.slotConversionOffset)}")
        /* Chain sync start point is the nearest block to the smallest resolution duration */
        val nearestBlock = candleStartPoints[Helpers.smallestDuration]?.plus(Helpers.slotConversionOffset)
            ?.let { chainDatabaseService.getBlockNearestToSlot(it) }
            ?: throw Exception("Tried finding point to synch from, using: ${candleStartPoints[Helpers.smallestDuration]}, nearestBlock not found, exiting")
        return InitialisationState(Point(nearestBlock.slot, nearestBlock.hash), candleStartPoints)
    }

    /* Align back to the beginning of nearest candle dtg */
    private fun getDiscreteCandleAdjustedTimes(time: Long): Map<Duration,Long> {
        return Helpers.allResoDurations.associateWith {
            Helpers.toNearestDiscreteDate(it, LocalDateTime.ofEpochSecond(time, 0 , Helpers.zoneOffset))
                .toEpochSecond(Helpers.zoneOffset)
        }
    }

    fun stopSync() {
        blockSync.stop()
    }

    /* analyse block for latest and historical price updates */
    private fun processBlock(block: Block) {
        log.debug("Processing block... ${block.header.headerBody.blockNumber}")
        val startTime = System.currentTimeMillis()

        val slotDateTime = Helpers.convertSlotToDtg(block.header.headerBody.slot)
        val timeUntilSynced = (LocalDateTime.now().toEpochSecond(Helpers.zoneOffset) - (slotDateTime.toEpochSecond(Helpers.zoneOffset)))
        val isBootstrapping = timeUntilSynced > TimeUnit.MINUTES.toSeconds(15)
        log.debug("Chain data still to synch: $timeUntilSynced [s], Is bootstrapping: $isBootstrapping")

        val allSwaps = mutableListOf<Swap>()

        val qualifiedTxMap = qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)

        /* Compute swaps and add/update assets and latest prices */
        qualifiedTxMap.forEach txloop@{ txDTO ->
            val swaps = dexClassifierMap[txDTO.dexCredential]?.computeSwaps(txDTO)
            log.debug("Computing swaps for dex: ${txDTO.dexCode}, ${txDTO.txHash}, Classifier: ${dexClassifierMap[txDTO.dexCredential]}, # swaps: ${swaps?.size}")

            if (!swaps.isNullOrEmpty()) {
                allSwaps.addAll(swaps)
                /* persist latest price from last swap, insertOrUpdate on a timer */
                latestPriceService.batchProcessLatestPrices(swaps.last())
            } else {
                // This occurs for minswap v2 tx currently
                log.debug("HAD A NULL SWAP, MEANING 0 QUANITY VALUE, for txHash: ${txDTO.txHash}")
            }
        }

        if (config.makeHistoricalData == true) {
            historicalPriceService.batchProcessHistoricalPrices(allSwaps, block.header.headerBody.slot, isBootstrapping)
        }

        if (!isBootstrapping) {
            log.info("Processed Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, ${block.header.headerBody.slot}, Total # of Txns >> ${block.transactionBodies.size}, Swaps >> ${allSwaps.size}, took: ${System.currentTimeMillis() - startTime} [ms]")
        }
    }

    /* Filter to transactions containing a DEX swap, resolve TxIn Refs to full input UTXO payloads */
    fun qualifyTransactions(blockSlot: Long, transactionBodies: List<TransactionBody>, transactionWitnesses: List<Witnesses>): List<FullyQualifiedTxDTO> {
        log.debug("transaction bodies: $transactionBodies")
        val filteredTxAndWitnesses: List<Pair<TransactionBody, Witnesses>> = transactionBodies.zip(transactionWitnesses)
            .filter { it.first.outputs
                .map { o -> o.address }
                .any { a -> dexPaymentCredentials.contains(Helpers.convertScriptAddressToPaymentCredential(a)) } }
        log.debug("Number dex swap tx: ${filteredTxAndWitnesses.size}")

        val qualifiedTx = filteredTxAndWitnesses
            .map { (txBody, witnesses) ->
                val dexCredentialMatched = txBody.outputs
                    .map { Helpers.convertScriptAddressToPaymentCredential(it.address) }
                    .first { dexPaymentCredentials.contains(it) }
                FullyQualifiedTxDTO(
                    txBody.txHash,
                    Helpers.resolveDexNumFromCredential(dexCredentialMatched),
                    dexCredentialMatched,
                    blockSlot,
                    runBlocking { chainDatabaseService.getInputUtxos(txBody.inputs) },
                    txBody.outputs,
                    witnesses)
            }.filter {
                it.inputUtxos.isNotEmpty()
            }
        log.debug("Number qualified tx: ${qualifiedTx.size}, ${qualifiedTx.map { it.txHash +"," + it.dexCode }}")
        return qualifiedTx
    }
}