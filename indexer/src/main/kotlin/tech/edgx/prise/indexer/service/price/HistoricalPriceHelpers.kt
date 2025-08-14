package tech.edgx.prise.indexer.service.price

import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.domain.Price
import tech.edgx.prise.indexer.model.PriceDTO
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

    enum class TriggerType { FINALISE, INITIALISE, UPDATE }

    data class TriggerDTO(
        val type: TriggerType,
        val candleDtg: LocalDateTime,
        val prices: List<Price>,
        val duration: Duration,
        val isBootstrapping: Boolean,
        val slot: Long
    )

    private fun passedDtgBoundary(recentDtg: LocalDateTime, previousDtg: LocalDateTime?): Boolean {
        return recentDtg != previousDtg
    }

    fun determineTriggers(
        isBootstrapping: Boolean,
        currentDtg: LocalDateTime,
        bufferedPrices: List<Price>,
        blockPrices: List<Price>,
        duration: Duration,
        previousCandleDtgState: Map<Duration, LocalDateTime>,
        isSubsequentTrigger: Boolean,
        slot: Long
    ): List<TriggerDTO> {
        val triggers = mutableListOf<TriggerDTO>()

        log.debug(
            "Determining triggers with: bootstrapping: $isBootstrapping, currentDtg: $currentDtg, bufferedPrices: ${bufferedPrices.size}, block-prices: ${blockPrices.size}, duration: $duration, previousDtgState: ${previousCandleDtgState[duration]}"
        )
        when (passedDtgBoundary(currentDtg, previousCandleDtgState[duration])) {
            true -> {
                when (isSubsequentTrigger) {
                    false -> {
                        val filteredPrices = bufferedPrices.filter { it.time < currentDtg.toEpochSecond(Helpers.zoneOffset) }
                        log.debug(
                            "Filtered prices #: ${filteredPrices.size} vs bufferedPrices: ${bufferedPrices.size}, equal: ${filteredPrices.size == bufferedPrices.size}, # new prices this round: ${blockPrices.size}"
                        )
                        triggers.add(
                            TriggerDTO(
                                type = TriggerType.FINALISE,
                                candleDtg = currentDtg.minus(duration),
                                prices = filteredPrices,
                                duration = duration,
                                isBootstrapping = isBootstrapping,
                                slot = slot
                            )
                        )
                    }
                    true -> {
                        triggers.add(
                            TriggerDTO(
                                type = TriggerType.FINALISE,
                                candleDtg = currentDtg.minus(duration),
                                prices = emptyList(),
                                duration = duration,
                                isBootstrapping = isBootstrapping,
                                slot = slot
                            )
                        )
                    }
                }
                if (!isBootstrapping) {
                    log.debug("Initialising next candles for dtg: $currentDtg, with starting prices, #: ${blockPrices.size}")
                    triggers.add(
                        TriggerDTO(
                            type = TriggerType.INITIALISE,
                            candleDtg = currentDtg,
                            prices = blockPrices,
                            duration = duration,
                            isBootstrapping = isBootstrapping,
                            slot = slot
                        )
                    )
                }
            }
            else -> {
                if (!isBootstrapping && blockPrices.isNotEmpty()) {
                    log.debug("Triggered candle update with new prices: ${bufferedPrices.size}, for nextDtg: $currentDtg")
                    triggers.add(
                        TriggerDTO(
                            type = TriggerType.UPDATE,
                            candleDtg = currentDtg,
                            prices = bufferedPrices,
                            duration = duration,
                            isBootstrapping = isBootstrapping,
                            slot = slot
                        )
                    )
                }
            }
        }
        return triggers
    }

    internal fun transformTradesToPrices(
        prices: List<Price>,
        toAsset: Asset
    ): List<PriceDTO> {
        return prices.mapNotNull { price ->
            val decimals = toAsset.decimals?.toDouble() ?: 0.0
            if (decimals < 0) {
                log.warn("Invalid decimals for asset: ${toAsset.unit}, decimals: $decimals")
                null
            } else {
                PriceDTO(
                    ldt = LocalDateTime.ofEpochSecond(price.time, 0, Helpers.zoneOffset),
                    price = price.price,
                    volume = (price.amount1.toFloat() / 10.0.pow(decimals)).toFloat()
                )
            }
        }
    }

    fun filterOutliersByGrubbsTest(rawPrices: List<PriceDTO>, lastCandle: CandleDTO?): List<PriceDTO> {
        var mutableGroupPrices = rawPrices.toMutableList()
        log.debug("# real prices before: ${mutableGroupPrices.size}")
        lastCandle?.let {
            val prevKnownValue = PriceDTO(price = listOf(it.close!!, it.open).average().toFloat(), volume = -1.0F)
            val prevKnownValues = List(3) { prevKnownValue }
            mutableGroupPrices.addAll(prevKnownValues)
        }
        var outlier: Float?
        while (StatsUtil.getOutlier(mutableGroupPrices.stream().map { f: PriceDTO -> f.price }
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

    fun filterOutliersByEMATest(groupPrices: List<PriceDTO>, ema: Double, variance: Double): List<PriceDTO> {
        log.debug("Filtering by EMA test: ${groupPrices}, EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        val filtered = groupPrices.filter {
            if (it.price != null) {
                log.debug(
                    "Testing now: EMA: $ema, price: ${it.price}, diff: ${(ema - it.price).absoluteValue}, 2std dev: ${2*sqrt(variance)}, filter it?: ${(ema - it.price).absoluteValue > 2*sqrt(variance)}"
                )
                (ema - it.price).absoluteValue <= 2*sqrt(variance)
            } else false
        }
        log.debug("Filtered size: ${filtered.size}")
        return filtered
    }

    fun determineClosePrice(filteredGroupPrices: List<PriceDTO>?): Float? {
        var latestPriceDTO: PriceDTO? = filteredGroupPrices?.last()
        if (filteredGroupPrices
                ?.map { gp -> gp.ldt }
                ?.filter { d -> d == latestPriceDTO?.ldt }
                ?.toList()?.size!! > 1) {
            latestPriceDTO = filteredGroupPrices
                .filter { gp: PriceDTO -> gp.ldt == latestPriceDTO?.ldt }
                .filter { p -> p.price != null }
                .reduce { a: PriceDTO, b: PriceDTO -> if (a.price!! < b.price!!) a else b }
            log.trace("Duplicate close on last slot, using: ${latestPriceDTO.price}")
        }
        return latestPriceDTO?.price
    }

    fun determineOpenPrice(
        lastCandle: CandleDTO?,
        rollingLastClose: Float?,
        candleDtgKey: LocalDateTime,
        firstPriceVal: Float?
    ): Float? {
        val open = if (lastCandle != null)
            if (lastCandle.time == candleDtgKey.toEpochSecond(Helpers.zoneOffset)) lastCandle.open
            else rollingLastClose ?: firstPriceVal
        else rollingLastClose ?: firstPriceVal
        log.trace(
            "Using Open: $open, last candle present: $lastCandle, " +
                    "Last candle being analysed: ${(if (lastCandle != null) lastCandle.time == candleDtgKey.toEpochSecond(
                        Helpers.zoneOffset
                    ) else "n/a")}, " +
                    "Last close: ${lastCandle?.close}, First candle in series: $firstPriceVal"
        )
        return open
    }

    fun calculateCandleFromSwaps(
        rawPrices: List<PriceDTO>,
        fromAsset: Asset,
        toAsset: Asset,
        lastCandle: CandleDTO?,
        candleDtg: LocalDateTime
    ): CandleDTO? {
        log.debug("Making candles for name: ${fromAsset.native_name}, with # raw data: ${rawPrices.size}, with lastCandle: $lastCandle")
        val startTime = System.currentTimeMillis()

        val filteredPrices = filterOutliersByGrubbsTest(rawPrices, lastCandle)
            .ifEmpty { listOf(PriceDTO(candleDtg, lastCandle?.close, 0.0F)) }
        log.trace("# prices before filtering: ${rawPrices.size}, after filtering: ${filteredPrices.size}")

        val volume: Float = rawPrices
            .map { d: PriceDTO -> d.volume }
            .fold(0.0F) { a: Float, b: Float -> a + b }

        val close = determineClosePrice(filteredPrices)
        val open = determineOpenPrice(lastCandle, lastCandle?.close, candleDtg, filteredPrices.firstOrNull()?.price)
        log.debug(
            "For time: ${candleDtg.toEpochSecond(Helpers.zoneOffset)}, Determining open price from; $lastCandle, lastClose: ${lastCandle?.close}, dtgKey: $candleDtg, FirstPrice: ${filteredPrices.firstOrNull()?.price}, Determined open; $open"
        )

        val candle = if (open != null && close != null) {
            val prices: MutableList<Float> = filteredPrices
                .filter { p -> p.price != null }
                .map { ph -> ph.price!! }
                .toMutableList()
            prices.add(open)
            CandleDTO(
                asset_id = fromAsset.id,
                quote_asset_id = toAsset.id,
                time = candleDtg.toEpochSecond(Helpers.zoneOffset),
                open = open,
                high = Collections.max(prices),
                low = Collections.min(prices),
                close = close,
                volume = volume
            )
        } else {
            log.trace("Open was null, no need for candle")
            null
        }
        log.debug("Made candle: $candle, took: ${(System.currentTimeMillis() - startTime) / 1000} [s]")
        return candle
    }
}