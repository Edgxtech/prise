package tech.edgx.prise.indexer.thread

import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.service.monitoring.MonitoringService
import tech.edgx.prise.indexer.service.price.LatestPriceService
import tech.edgx.prise.indexer.util.Helpers
import kotlin.coroutines.CoroutineContext

/* Updated asset last prices are batched on a timer */
class LatestPriceBatcher(config: Config) : CoroutineScope, KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    private var job: Job = Job()
    override val coroutineContext: CoroutineContext get() = Dispatchers.Default + job
    private val latestPriceService: LatestPriceService by inject { parametersOf(config) }
    private val monitoringService: MonitoringService by inject { parametersOf(config.metricsServerPort) }
    private var config = config
    private var errorCount = 0
    private val MAX_ERRORS = 10

    fun cancel() {
        job.cancel()
    }

    fun start() = launch(CoroutineName("latest_price_batcher")) {
        while (isActive) {
            try {
                /* update known asset prices from buffered latest swaps */
                val totalUpdatedAssets = latestPriceService.updateAssetsNow()

                /* make any new assets */
                val totalNewAssets = if (latestPriceService.newAssetBuffer.isNotEmpty()) {
                    log.debug("Have some buffered new assets")
                    latestPriceService.makeNewAssetsNow()
                } else 0

                /* increment monitor */
                monitoringService.incrementCounter(Helpers.ASSET_LIVE_SYNC_LABEL)
                log.info("[FINISHED] latest price batcher, new assets: $totalNewAssets, updated: $totalUpdatedAssets")
                delay(config.latestPricesLivesyncUpdateIntervalSeconds * 1000)
                errorCount = 0
            } catch (e: Exception) {
                errorCount++
                if (errorCount > MAX_ERRORS) {
                    throw Exception("Exception syncing asset data, exceed MAX error count, exiting thread ...")
                } else {
                    log.error("Exception syncing asset data: ${e.message}: $e, pausing and continuing to try another ${MAX_ERRORS - errorCount} times...")
                    delay(15000)
                }
            }
        }
        println("coroutine done")
    }
}