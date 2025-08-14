package tech.edgx.prise.indexer.thread

import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.event.BlockReceivedEvent
import tech.edgx.prise.indexer.event.EventBus
import tech.edgx.prise.indexer.service.PriceRecord
import tech.edgx.prise.indexer.service.PriceService
import tech.edgx.prise.indexer.util.Helpers
import tech.edgx.prise.indexer.util.StatsUtil
import java.util.stream.Collectors

class OutlierDetectionWorker : KoinComponent {
    private val log = LoggerFactory.getLogger(OutlierDetectionWorker::class.java)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val priceService: PriceService by inject()
    private val eventBus: EventBus by inject()
    private var lastOutlierDetectionSlot: Long? = null
    private val slotInterval = 3600L // seconds of block time (1hr)

    fun start() {
        scope.launch {
            eventBus.events.collect { event ->
                if (event is BlockReceivedEvent) {
                    val currentSlot = event.block.header.headerBody.slot
                    if (shouldRunOutlierDetection(currentSlot)) {
                        detectOutliers(currentSlot)
                        lastOutlierDetectionSlot = currentSlot
                    }
                }
            }
        }
        log.info("Outlier detection worker started")
    }

    private fun shouldRunOutlierDetection(currentSlot: Long): Boolean {
        return lastOutlierDetectionSlot == null || (currentSlot - lastOutlierDetectionSlot!!) >= slotInterval
    }

    private suspend fun detectOutliers(currentSlot: Long) {
        log.debug("Running outlier detection for last 24 hours of data at slot $currentSlot")
        val startTime = System.currentTimeMillis()
        val fromTime = currentSlot - Helpers.slotConversionOffset - 24 * 3600
        var totalOutliers = 0

        try {
            // Fetch distinct asset pairs
            val pairs = priceService.getDistinctAssetPairs(fromTime)
            log.debug("Found {} asset pairs to process", pairs.size)

            pairs.forEach { (assetId, quoteAssetId) ->
                // Fetch prices for the pair
                val prices = priceService.getPricesForPair(assetId, quoteAssetId, fromTime)
                if (prices.size < 3) {
                    log.debug("Skipping pair ({}, {}): insufficient data ({} prices)", assetId, quoteAssetId, prices.size)
                    return@forEach
                }

                // Fetch recent average price for context
                val recentAvgPrice = priceService.getRecentAveragePrice(assetId, quoteAssetId, fromTime)

                // Apply Grubbs test
                val nonOutlierPrices = filterOutliersByGrubbsTest(prices, recentAvgPrice)
                val outlierKeys = prices
                    .map { Triple(it.time, it.txId, it.txSwapIdx) }
                    .filter { key -> key !in nonOutlierPrices.map { Triple(it.time, it.txId, it.txSwapIdx) } }

                if (outlierKeys.isNotEmpty()) {
                    // Update outlier flags
                    val updatedRows = priceService.updateOutliers(outlierKeys)
                    totalOutliers += outlierKeys.size
                    log.debug("Marked $updatedRows outliers for pair ($assetId, $quoteAssetId)")
                }
            }
        } catch (e: Exception) {
            log.error("Error during outlier detection", e)
        }
        log.info("Outlier detection completed in ${System.currentTimeMillis() - startTime} ms, found $totalOutliers outliers")
    }

    private data class ExtendedPriceRecord(
        val time: Long,
        val txId: Long,
        val txSwapIdx: Int,
        val price: Float,
        val volume: Float
    )

    private fun filterOutliersByGrubbsTest(prices: List<PriceRecord>, recentAvgPrice: Double?): List<PriceRecord> {
        var mutableGroupPrices = prices.map {
            ExtendedPriceRecord(it.time, it.txId, it.txSwapIdx, it.price.toFloat(), 1.0F)
        }.toMutableList()
        log.debug("# real prices before: ${mutableGroupPrices.size}")

        // Add synthetic prices from recent average price if available
        recentAvgPrice?.let { avgPrice ->
            val syntheticPrice = ExtendedPriceRecord(
                time = -1, // Dummy values for synthetic records
                txId = -1,
                txSwapIdx = -1,
                price = avgPrice.toFloat(),
                volume = -1.0F
            )
            val syntheticPrices = List(3) { syntheticPrice }
            mutableGroupPrices.addAll(syntheticPrices)
        }

        // Iteratively remove outliers
        var outlier: Float?
        while (StatsUtil.getOutlier(
                mutableGroupPrices.stream().map { f: ExtendedPriceRecord -> f.price }
                    .collect(Collectors.toList())
            ).also { outlier = it } != null
        ) {
            log.trace("Found outlier value: $outlier, removing")
            val finalOutlier = outlier
            mutableGroupPrices = mutableGroupPrices.filter { f -> f.price != finalOutlier }.toMutableList()
        }

        // Remove synthetic prices if any were added
        mutableGroupPrices = mutableGroupPrices.filter { it.volume >= 0 }.toMutableList()

        log.debug("# real prices after: ${mutableGroupPrices.size}")
        return mutableGroupPrices.map {
            PriceRecord(it.time, it.txId, it.txSwapIdx, it.price.toDouble())
        }
    }

    fun stop() {
        log.info("Stopping outlier detection worker")
        scope.cancel()
    }
}