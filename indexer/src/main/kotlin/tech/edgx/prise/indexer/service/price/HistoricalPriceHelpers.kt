package tech.edgx.prise.indexer.service.price

import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.domain.LatestCandlesView
import tech.edgx.prise.indexer.model.PriceHistoryDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.util.Helpers
import tech.edgx.prise.indexer.util.StatsUtil
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.stream.Collectors
import kotlin.math.absoluteValue
import kotlin.math.pow
import kotlin.math.sqrt

object HistoricalPriceHelpers {

    private val log = LoggerFactory.getLogger(javaClass)

    enum class TriggerType{FINALISE, INITIALISE, UPDATE}

    data class TriggerDTO(val type: TriggerType,
                          val candleDtg: LocalDateTime,
                          val swaps: List<Swap>,
                          val duration: Duration,
                          val isBootrapping: Boolean,
                          val slot: Long)

    private fun passedDtgBoundary(recentDtg: LocalDateTime, previousDtg: LocalDateTime?): Boolean {
        return (recentDtg != previousDtg)
    }

    /* Triggers :
    *   - FINALISE: Chain is bootstrapping - trigger only when block sync has passed over the candle boundary for the previous dtg
    *   - INITIALISE: Chain is in synch and tracking ~latest blocks - trigger when block sync has passed over the next candle dtg boundary
    *   - UPDATE: Chain is in synch and tracking ~latest blocks - trigger for each block that has swaps (called frequently to update blocks in live time) */
    fun determineTriggers(isBootrapping: Boolean, currentDtg: LocalDateTime, bufferedSwaps: List<Swap>, blockSwaps: List<Swap>, duration: Duration, previousCandleDtgState: Map<Duration,LocalDateTime>, isSubsequentTrigger: Boolean, slot: Long): List<TriggerDTO> {
        val triggers = mutableListOf<TriggerDTO>()

        log.debug("Determining triggers with: bootstrapping: $isBootrapping, currentDtg: $currentDtg, bufferedSwaps: $bufferedSwaps, block-swaps: $blockSwaps, duration: $duration, previousDtgState: $${previousCandleDtgState[duration]}")
        when (passedDtgBoundary(currentDtg, previousCandleDtgState[duration])) {
            true -> {
                when (isSubsequentTrigger) {
                    false -> {
                        val filteredSwaps = bufferedSwaps.filter { Helpers.convertSlotToDtg(it.slot) < currentDtg }
                        log.debug("Filtered swaps #: ${filteredSwaps.size} vs bufferedSwaps: ${bufferedSwaps.size}, equal: ${filteredSwaps.size == bufferedSwaps.size}, # new swaps this round: ${blockSwaps.size}")
                        triggers.add(
                            TriggerDTO(
                                TriggerType.FINALISE,
                                currentDtg.minus(duration),
                                filteredSwaps,
                                duration,
                                isBootrapping,
                                slot
                            )
                        )
                    }
                    true -> {
                        triggers.add(
                            TriggerDTO(
                                TriggerType.FINALISE,
                                currentDtg.minus(duration),
                                emptyList(),
                                duration,
                                isBootrapping,
                                slot
                            )
                        )
                    }
                }
                if (!isBootrapping) {
                    /* Initialise a starting candle */
                    log.debug("Initialising next candles for dtg: $currentDtg, with starting swaps, #: ${blockSwaps.size}")
                    triggers.add(
                        TriggerDTO(
                            TriggerType.INITIALISE,
                            currentDtg,
                            blockSwaps,
                            duration,
                            isBootrapping,
                            slot
                        )
                    )
                }
            }
            else -> {
                if (!isBootrapping && blockSwaps.isNotEmpty()) {
                    /* Update candles with latest blockSwaps */
                    log.debug("Triggered candle update with new swaps: ${bufferedSwaps.size}, for nextDtg: $currentDtg}")
                    triggers.add(
                        TriggerDTO(
                            TriggerType.UPDATE,
                            currentDtg,
                            bufferedSwaps,
                            duration,
                            isBootrapping,
                            slot
                        )
                    )
                }
            }
        }
        return triggers
    }

    /* Convert on-chain data into date,price,volumes */
    internal fun transformTradesToPrices(
        swaps: List<Swap>?,
        fromAsset: Asset,
        toAsset: Asset): List<PriceHistoryDTO>? {
        return swaps
            ?.map { price ->
                PriceHistoryDTO(
                    LocalDateTime.ofEpochSecond(
                        price.slot.minus(Helpers.slotConversionOffset),
                        0,
                        Helpers.zoneOffset
                    ),
                    Helpers.calculatePriceInAsset1(
                        price.amount1,
                        toAsset.decimals,
                        price.amount2,
                        fromAsset.decimals
                    ),
                    price.amount1.toDouble().div(10.0.pow(6.0)))
            }?.toList()
    }

    fun filterOutliersByGrubbsTest(rawPrices: List<PriceHistoryDTO>, lastCandle: CandleDTO? ): List<PriceHistoryDTO> {
        var mutableGroupPrices = rawPrices.toMutableList()
        log.debug("# real prices before: ${mutableGroupPrices.size}")
        lastCandle?.let {
            // Optimised with; adding 3 prev values & grubbs filtering with 0.85 significance lvl
            val prevKnownValue = PriceHistoryDTO(price = listOf(lastCandle.close!!,lastCandle.open!!).average(), volume=-1.0)
            val prevKnownValues = List(3) { prevKnownValue }
            mutableGroupPrices.addAll(prevKnownValues)
        }
        var outlier: Double?
        while (StatsUtil.getOutlier(mutableGroupPrices.stream().map { f: PriceHistoryDTO -> f.price }
                .collect(Collectors.toList()))
                .also { outlier = it } != null) {
            log.trace("Found outlier value: $outlier, removing")
            val finalOutlier = outlier
            mutableGroupPrices = mutableGroupPrices.filter { f -> f.price !== finalOutlier }.toMutableList()
        }
        lastCandle?.let {
            mutableGroupPrices = mutableGroupPrices.filter { it.volume >= 0 }.toMutableList()
        }
        log.debug("# real prices after: ${mutableGroupPrices.size}")
        return mutableGroupPrices.toList()
    }

    fun filterOutliersByEMATest(groupPrices: List<PriceHistoryDTO>, ema: Double, variance: Double): List<PriceHistoryDTO> {
        log.debug("Filtering by EMA test: ${groupPrices}, EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        val filtered = groupPrices.filter {
           if (it.price != null) {
               log.debug("Testing now: EMA: $ema, price: ${it.price}, diff: ${(ema - it.price).absoluteValue}, 2std dev: ${2*sqrt(variance)}, filter it?: ${(ema - it.price).absoluteValue > 2*sqrt(variance)}")
               (ema - it.price).absoluteValue <= 2*sqrt(variance)
           }
           else false
        }
        log.debug("Filtered size: ${filtered.size}")
        return filtered
    }

    /* If the date of last price data is duplicated (possible since time delineated by ~20s slot intervals), pick the smallest value to use for candle */
    fun determineClosePrice(filteredGroupPrices: List<PriceHistoryDTO>?): Double? {
        var latestPriceDTO: PriceHistoryDTO? = filteredGroupPrices?.last()
        if (filteredGroupPrices
                ?.map { gp -> gp.ldt }
                ?.filter { d -> d == latestPriceDTO?.ldt }
                ?.toList()?.size!! > 1) {
            latestPriceDTO = filteredGroupPrices
                .filter { gp: PriceHistoryDTO -> (gp.ldt == latestPriceDTO?.ldt) }
                .filter { p -> p.price != null }
                .reduce { a: PriceHistoryDTO, b: PriceHistoryDTO -> if (a.price!! < b.price!!) a else b }
            log.trace("Duplicate close on last slot, using: " + latestPriceDTO.price)
        }
        return latestPriceDTO?.price
    }

    /* If last candle provided and the candle I need to make IS the latest candle, need to preserve its original OPEN price (since this is built up historically)
    *  If last candle provided and the candle I need to make IS NOT the latest candle, use the last close price (this is normal continuation for candles
    *  Else use the very first data point (this is a fresh start) */
    fun determineOpenPrice(
        lastCandle: CandleDTO?,
        rollingLastClose: Double?,
        candleDtgKey: LocalDateTime,
        firstPriceVal: Double?): Double? {
        val open = if (lastCandle != null)
            if (lastCandle.time == candleDtgKey.toEpochSecond(Helpers.zoneOffset)) lastCandle.open
            else rollingLastClose ?: firstPriceVal
        else rollingLastClose ?: firstPriceVal
        log.trace("Using Open: $open, last candle present: ${lastCandle}, " +
                "Last candle being analysed: ${(if (lastCandle != null) lastCandle.time == candleDtgKey.toEpochSecond(
                    Helpers.zoneOffset) else "n/a")}, " +
                "Last close: ${lastCandle?.close}, First candle in series: " + firstPriceVal)
        return open
    }

    /* Calculate candles from raw swaps on-chain; used for generating the lowest resolution */
    fun calculateCandleFromSwaps(
        rawPrices: List<PriceHistoryDTO>,
        fromAsset: Asset,
        lastCandle: CandleDTO?,
        candleDtg: LocalDateTime): CandleDTO? {
        //emaVarianceState: Pair<Double,Double>?): CandleDTO? {
        log.debug("Making candles for name: ${fromAsset.native_name}, with # raw data: ${rawPrices.size}, with lastCandle: ${lastCandle}")
        val startTime = System.currentTimeMillis()

        /* Filter outliers */
        val filteredPrices = filterOutliersByGrubbsTest(rawPrices, lastCandle)
             /* This is a bit awkward, but the outlier filtering can remove all data points, I just add the lastClose as a single entry
                so that a continuation candle is made, however unlike other continuation candles, this will still have the original raw volume */
            .ifEmpty { listOf(PriceHistoryDTO(candleDtg, lastCandle?.close, 0.0)) }
        log.trace("# prices before filtering: ${rawPrices.size}, after filtering: ${filteredPrices.size}")

        /* compute volume from the UNFILTERED data; groupedRawPrices */
        val volume: Double = rawPrices
            .map { d: PriceHistoryDTO -> d.volume }
            .fold(0.0) { a: Double, b: Double -> a + b }

        /* pick a single close, since timestamp is a slot interval, might be duplicates */
        val close = determineClosePrice(filteredPrices)

        /* determine open; either the last close, first data point, or if is the last candle itself retain as lastOpen */
        val open = determineOpenPrice(lastCandle, lastCandle?.close, candleDtg, filteredPrices[0].price)
        log.debug("For time: ${candleDtg.toEpochSecond(Helpers.zoneOffset)}, Determining open price from; $lastCandle, lastClose: ${lastCandle?.close}, dtgKey: $candleDtg, FirstPrice: ${filteredPrices[0].price}, Determined open; $open")

        /* Compute and return */
        val candle = if (open != null) {
            val prices: MutableList<Double> = filteredPrices
                .filter { p -> p.price != null }
                .map { ph -> ph.price!! }
                .toMutableList()
            prices.add(open) // Include for min/max calculation
            CandleDTO(
                fromAsset.unit,
                candleDtg.toEpochSecond(Helpers.zoneOffset),
                open,
                Collections.max(prices),
                Collections.min(prices),
                close,
                volume
            )
        } else {
            log.trace("Open was null, no need for candle")
            null
        }
        log.debug("Made candle: ${candle}, took: ${(System.currentTimeMillis() - startTime) / 1000} [s]")
        return candle
    }

    /* Calculate candles from a set of lower reso candles */
    fun calculateCandleFromSubCandles(
        candleSet: List<LatestCandlesView>,
        fromAssetUnit: String,
        lastCandle: CandleDTO?,
        candleDtg: LocalDateTime): CandleDTO? {

        log.debug("Making candles for name: $fromAssetUnit, with # sub-candles: ${candleSet.size}, with lastCandle: $lastCandle")
        val startTime = System.currentTimeMillis()

        /* Compute volume */
        val volume: Double = candleSet
            .map { d -> d.volume }
            .fold(0.0) { a, b -> a + b }

        /* determine open; either the last close, first data point, or if is the last candle itself retain as lastOpen */
        val open = determineOpenPrice(lastCandle, lastCandle?.close, candleDtg, candleSet[0].open)
        log.debug("For time: ${candleDtg.toEpochSecond(Helpers.zoneOffset)}, Determining open price from; $lastCandle, lastClose: ${lastCandle?.close}, dtgKey: $candleDtg, FirstPrice: ${candleSet[0].open}, Determined open; $open")

        /* Compute and return */
        val candle = if (open != null) {
            val maxPrices: List<Double> = candleSet
                .filter { p -> p.high != null }
                .map { ph -> ph.high!! }
            val minPrices: List<Double> = candleSet
                .filter { p -> p.low != null }
                .map { ph -> ph.low!! }
            CandleDTO(
                fromAssetUnit,
                candleDtg.toEpochSecond(Helpers.zoneOffset),
                open,
                Collections.max(maxPrices),
                Collections.min(minPrices),
                candleSet.last().close,
                volume
            )
        } else {
            log.trace("Open was null, no need for candle")
            null
        }
        log.debug("Made candle: ${candle}, took: ${(System.currentTimeMillis() - startTime) / 1000} [s]")
        return candle
    }

    /*
        Experimenting alternate outlier filter - Not using currently
    */
    fun getNextEma(ema: Double, variance: Double, newVal: Double): Pair<Double,Double> {
        val N = 3
        val smoothing = 2
        val k = smoothing/(N+1).toDouble()
        val newEma = ema*(1-k) + newVal*k
        /* New variance = Lambda*σ^2 + (1-Lambda)u^2 */
        val lambda = 0.9
        val newVariance = lambda*variance + (1-lambda)*((newVal - ema).pow(2))
        return Pair(newEma,newVariance)
    }
}