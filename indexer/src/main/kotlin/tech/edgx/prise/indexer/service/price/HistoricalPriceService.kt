package tech.edgx.prise.indexer.service.price

import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.component.get
import org.koin.core.parameter.parametersOf
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.*
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.CandleService
import tech.edgx.prise.indexer.service.monitoring.MonitoringService
import tech.edgx.prise.indexer.util.Helpers
import tech.edgx.prise.indexer.util.HistoricalCandleResolutions
import tech.edgx.prise.indexer.service.price.HistoricalPriceHelpers.TriggerDTO
import tech.edgx.prise.indexer.service.price.HistoricalPriceHelpers.TriggerType
import java.time.LocalDateTime
import java.util.*
import java.time.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.math.pow

class HistoricalPriceService(private val config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass::class.java)

    private val monitoringService: MonitoringService by inject { parametersOf(config.metricsServerPort) }
    private val candleService: CandleService = get { parametersOf(config) }
    var assetService: AssetService = get { parametersOf(config) }
    //private val assetService: AssetService by inject { parametersOf(config) }
    private val latestPriceService: LatestPriceService by inject { parametersOf(config) }

    var CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SWAPS = 10
    var CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SUBS = 50

    val widerDurations = HistoricalCandleResolutions.entries
        .map { Helpers.convertResoDuration(it.code) }
        .filter { d -> d != Helpers.smallestDuration }.sorted()

    val bufferedSwaps = Collections.synchronizedList(CopyOnWriteArrayList<Swap>())

    var previousCandleDtgState = Collections.synchronizedMap(ConcurrentHashMap<Duration, LocalDateTime>())

    val emaVarianceState = Collections.synchronizedMap(ConcurrentHashMap<String,Pair<Double,Double>>())

    fun initialiseLastCandleState(candleStartPoints: Map<Duration,Long>) {
        log.info("Initialising last candle dtg state with: $candleStartPoints")
        previousCandleDtgState = candleStartPoints.map {
            it.key to LocalDateTime.ofEpochSecond(it.value, 0, Helpers.zoneOffset)
        }.toMap()
    }

    fun batchProcessHistoricalPrices(swaps: List<Swap>, slot: Long, isBootstrapping: Boolean) {
        log.debug("Buffering # swaps: ${swaps.size}, accumulated #: ${bufferedSwaps.size}")
        bufferedSwaps.addAll(swaps)

        val currentSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, Helpers.convertSlotToDtg(slot))

        val triggers: List<TriggerDTO> = HistoricalPriceHelpers.determineTriggers(isBootstrapping, currentSlotDtg, bufferedSwaps, swaps, Helpers.smallestDuration, previousCandleDtgState, false, slot)

        if (triggers.map { it.type }.contains(TriggerType.FINALISE)) {
            /* restart the buffer with only the swaps in this block */
            bufferedSwaps.clear()
            bufferedSwaps.addAll(swaps)

            /* Update dtg state */
            previousCandleDtgState[Helpers.smallestDuration] = currentSlotDtg
        }

        triggers.forEach {
            triggerCandleMakes(it)
        }
    }

    private fun triggerCandleMakes(trigger: TriggerDTO) {
        /* Due to batch making new assets, there may be some leftover to be made first */
        log.debug("Pre-making new assets prior to making candles, new asset buffer, #: ${latestPriceService.newAssetBuffer.size}")
        if (latestPriceService.newAssetBuffer.isNotEmpty()) {
            log.debug("Still had some leftover new assets to make, # ${latestPriceService.newAssetBuffer.size}")
            latestPriceService.makeNewAssetsNow()
        }

        /* For the smallest reso duration, make from swaps directly */
        triggerCandleMakeFromSwaps(trigger.candleDtg, trigger.duration, trigger.swaps)

        /* For alternate reso durations, check and make from sub-candles if required */
        widerDurations.forEach { duration ->
            val currentSlotAltResoDtg = Helpers.toNearestDiscreteDate(duration, Helpers.convertSlotToDtg(trigger.slot)) //trigger.candleDtg)
            /* compute subsequent triggers */
            val subsequentTriggers: List<TriggerDTO> = HistoricalPriceHelpers.determineTriggers(trigger.isBootrapping, currentSlotAltResoDtg, emptyList(), trigger.swaps, duration, previousCandleDtgState, true, trigger.slot)

            if (subsequentTriggers.map { it.type }.contains(TriggerType.FINALISE)) {
                /* Update dtg state */
                previousCandleDtgState[duration] = currentSlotAltResoDtg
            }

            log.debug("Subsequent triggers: $subsequentTriggers")
            subsequentTriggers.forEach {
                log.debug("Triggering subsequent candle make: $it")
                triggerCandleMakeFromSubs(it.candleDtg, it.duration)
            }
        }
    }

    private fun triggerCandleMakeFromSwaps(candleDtg: LocalDateTime, duration: Duration, swaps: List<Swap>) {
        /* make and persist candles */
        val candlesMade = makeCandlesFromSwaps(candleDtg, duration, swaps)
        log.debug("[FINISHED] Candle make for duration: $duration, dtg: $candleDtg, #: ${candlesMade.size}")

        /* Fill in any candles that didn't have swaps data, as continuation from prev close */
        populateContinuationCandles(candleDtg, duration, candlesMade.map { it.symbol })

        monitoringService.incrementCounter("${Helpers.HISTORICAL_CANDLE_MAKE_COUNT}_${duration}")
    }

    private fun triggerCandleMakeFromSubs(candleDtg: LocalDateTime, duration: Duration) {
        /* make and persist candles */
        val candlesMade = makeCandlesFromSubCandles(candleDtg, duration)
        log.debug("[FINISHED] Candle make for duration: $duration, dtg: $candleDtg, #: ${candlesMade.size}")

        /* Fill in any candles that didn't have swaps data, as continuation from prev close */
        populateContinuationCandles(candleDtg, duration, candlesMade.map { it.symbol })

        monitoringService.incrementCounter("${Helpers.HISTORICAL_CANDLE_MAKE_COUNT}_${duration}")
    }

    /* Candle generator from raw swaps; used to make the smallest reso candles */
    fun makeCandlesFromSwaps(candleDtg: LocalDateTime, duration: Duration, rawSwaps: List<Swap>): List<CandleDTO> {
        /* For each unique asset in the latest queued swaps, make candles, for all other assets add a continuation candle */
        val swapsGroupedByAsset: Map<String,List<Swap>> = rawSwaps.groupBy { it.asset2Unit }

        /* Pull all last candles */
        val lastCandlesMap: Map<String,CandleDTO> = candleService.getCandlesAtTime(duration, candleDtg.minus(duration).toEpochSecond(Helpers.zoneOffset))
                .associateBy { it.symbol }
        log.debug("# unique assets for this 15min period: ${swapsGroupedByAsset.keys.size}, latest candle map, #: ${lastCandlesMap.size}")

        val candlesBuffer = Collections.synchronizedList(mutableListOf<CandleDTO>())
        val allCandles = mutableListOf<CandleDTO>()

        swapsGroupedByAsset.forEach tokenloop@{ unit, swaps ->
            val lastCandle = lastCandlesMap.get(unit)
            log.debug("Populating LATEST candles for: $unit, latest candle: ${lastCandle}, # swaps on-chain: ${swaps.size}")

            val fromAsset = assetService.getAssetByUnit(unit)
            val toAsset = Helpers.adaAsset
            log.debug("Using asset data: $fromAsset")
            if (fromAsset == null) {
                /* shouldn't occur, if latestPriceService is functioning correctly */
                log.warn("An asset not in db, not making candles: $unit / ${Helpers.ADA_ASSET_UNIT}")
                return@tokenloop
            }

            val priceHistories = HistoricalPriceHelpers.transformTradesToPrices(swaps, fromAsset, toAsset)

            if (!priceHistories.isNullOrEmpty()) {
                val candle = HistoricalPriceHelpers.calculateCandleFromSwaps(
                    priceHistories,
                    fromAsset,
                    lastCandle,
                    candleDtg
                )
                if (candle != null) {
                    candlesBuffer.add(candle)
                    allCandles.add(candle)
                }

                /* Execute candle updates in batches */
                if (candlesBuffer.size >= CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SWAPS) {
                    log.debug("Persisting # batch candles: ${candlesBuffer.size}")
                    candleService.persistOrUpdate(candlesBuffer, duration)
                    log.debug("Finished persisting")
                    candlesBuffer.clear()
                    log.debug("Cleared candles, new size: ${candlesBuffer.size}")
                }
            }
        }
        // Persist any leftover buffered candles
        if (candlesBuffer.isNotEmpty()) {
            log.debug("Had leftover candles to persist, for reso: ${duration} #: ${candlesBuffer.size}")
            candleService.persistOrUpdate(candlesBuffer, duration)
        }
        return allCandles
    }

    /* Candle generator; from sub-candles */
    fun makeCandlesFromSubCandles(candleDtg: LocalDateTime, duration: Duration): List<CandleDTO> {
        log.debug("Making candles from sub candles: $candleDtg, $duration")
        /* Pull all last candles */
        val lastCandlesMap: Map<String,CandleDTO> = candleService.getCandlesAtTime(duration, candleDtg.minus(duration).toEpochSecond(Helpers.zoneOffset))
                .associateBy { it.symbol }
        log.debug("latest candle map, #: ${lastCandlesMap.size}")

        /* Determine the interval of subcandles necessary to build a higher resolution candle */
        val subCandleInterval = when (duration) {
            Duration.ofHours(1) -> 3600 //4*15*60
            Duration.ofDays(1) -> 86400 //24*60*60
            Duration.ofDays(7) -> 604800 //7*24*60*60
            else -> {
                log.error("Unknown duration passed, returning")
                return listOf()
            }
        }
        log.debug("subCandleInterval : $subCandleInterval")

        /* find next duration step down */
        val subCandleDuration = HistoricalCandleResolutions.entries
            .map { Helpers.convertResoDuration(it.code) }
            .first { d -> d < duration }
        log.debug("Subn candle duration: $subCandleDuration, should be one step down from $duration")

        val from = candleDtg.toEpochSecond(Helpers.zoneOffset)
        val subCandles: List<LatestCandlesView> =
            candleService.getNCandlesBetweenTimes(subCandleDuration, from, from + subCandleInterval)
        log.debug("Using candleTime: ${candleDtg.toEpochSecond(Helpers.zoneOffset)}, subcandle interval: $subCandleInterval, sub-duration: $subCandleDuration, All latest Sub candles: ${subCandles.size}")

        val subCandlesGroupedByAsset: Map<String, List<LatestCandlesView>> = subCandles.groupBy { it.symbol }

        val candlesBuffer = mutableListOf<CandleDTO>()
        val allCandles = mutableListOf<CandleDTO>()

        subCandlesGroupedByAsset.forEach tokenloop@{ unit, assetSubCandles ->
            val lastCandle = lastCandlesMap[unit]
            log.debug("Populating LATEST candles for: $unit, last candle: $lastCandle, # sub candles: ${subCandles.size}")

            val candle = HistoricalPriceHelpers.calculateCandleFromSubCandles(
                assetSubCandles,
                unit,
                lastCandle,
                candleDtg
            )
            if (candle != null) {
                candlesBuffer.add(candle)
                allCandles.add(candle)
            }
            if (candlesBuffer.size > CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SUBS) {
                log.debug("Persisting # batch candles, for reso: ${duration}: ${candlesBuffer.size}")
                candleService.persistOrUpdate(candlesBuffer, duration)
                log.debug("Finished persisting super candles for duration: $duration")
                candlesBuffer.clear()
                log.debug("Cleared candles, new size: ${candlesBuffer.size}")
            }
        }
        /* Persist any leftover buffered candles */
        if (candlesBuffer.isNotEmpty()) {
            log.debug("Had leftover candles to persist, for reso: ${duration} #: ${candlesBuffer.size}")
            candleService.persistOrUpdate(candlesBuffer, duration)
        }
        return allCandles
    }

    fun populateContinuationCandles(candleDtg: LocalDateTime, resoDuration: Duration, candleSymbolsMade: List<String>) {
        candleService.populateContinuationCandles(candleDtg, resoDuration, candleSymbolsMade)
        log.debug("[FINISHED] Batch populating continuations candles for candle dtg $candleDtg, ${candleDtg.toEpochSecond(Helpers.zoneOffset)}")
    }

    /*
        Experimenting alternate outlier filter - Not using currently
    */
    fun updateEmaVarianceState(unit: String, newVal: Double, time: Long) {
        val thisEMAVarianceState = emaVarianceState[unit]
        log.debug("EMA variance state: ${emaVarianceState[unit]}")
        when (thisEMAVarianceState==null) {
            true -> {
                // Keep trying to initialise
                initialiseEmaVarianceState(unit, time)
            }
            false -> {
                /* Recursive update of the EMA/Variance */
                HistoricalPriceHelpers.getNextEma(thisEMAVarianceState.first, thisEMAVarianceState.second, newVal).let {
                    emaVarianceState[unit]=Pair(it.first, it.second)
                }
                log.debug("Updated EMA state, for symbol: $unit, ${emaVarianceState[unit]}")
            }
        }
    }

    /*
        Experimenting alternate outlier filter - Not using currently
    */
    fun initialiseEmaVarianceState(unit: String, time: Long) {
        /* Wait until N prior candles are available */
        val lastNCloses = candleService.getNClosesForSymbolAtTime(Duration.ofMinutes(15), 5, unit, time)
        log.debug("Last N closes: $lastNCloses")
        if (lastNCloses.size>=5) {
            val initialEma = lastNCloses.average()
            val initialVariance = lastNCloses.map { (it - initialEma).pow(2) }.average()
            log.info("Initialising EMA state, for symbol: $unit, $initialEma, $initialVariance")
            emaVarianceState[unit]=Pair(initialEma, initialVariance)
        }
    }
}