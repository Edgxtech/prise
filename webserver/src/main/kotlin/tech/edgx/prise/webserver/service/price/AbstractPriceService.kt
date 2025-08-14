package tech.edgx.prise.webserver.service.price

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.slf4j.LoggerFactory
import tech.edgx.prise.webserver.model.prices.AssetPrice
import tech.edgx.prise.webserver.model.prices.CandleResponse
import tech.edgx.prise.webserver.model.prices.HistoricalCandlesRequest
import tech.edgx.prise.webserver.model.prices.LatestPricesRequest
import tech.edgx.prise.webserver.util.Helpers
import jakarta.annotation.Resource
import tech.edgx.prise.webserver.service.AssetService
import tech.edgx.prise.webserver.service.CandleService
import tech.edgx.prise.webserver.service.LatestPriceService
import java.time.LocalDateTime
import java.time.ZoneOffset

interface PriceService {
    fun getLatestPrices(latestPricesRequest: LatestPricesRequest): List<AssetPrice>
    fun getCandles(historicalCandlesRequest: HistoricalCandlesRequest): List<CandleResponse>
    fun getAllSupportedPairs(): Set<Pair<String, String>>
    fun getDistinctAssets(): Set<String>
}

abstract class AbstractPriceService(
    @Resource(name = "candleService") protected open val candleService: CandleService,
    @Resource(name = "assetService") protected open val assetService: AssetService,
    @Resource(name = "latestPriceService") protected open val latestPriceService: LatestPriceService
) : PriceService {
    protected val log = LoggerFactory.getLogger(this::class.java)
    protected val dexLaunchTime = LocalDateTime.of(2022, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC)
    protected val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    protected val fallbackDays = 30L * 24 * 3600 // 30 days in seconds
    protected val defaultQuote = "ADA"

    override fun getDistinctAssets(): Set<String> {
        return assetService.getDistinctAssets()
    }

    protected fun parsePairs(latestPricesRequest: LatestPricesRequest): Set<Pair<String, String>> {
        return latestPricesRequest.symbols.map { pair ->
            val parts = pair.split(":")
            when {
                parts.size == 2 -> parts[0] to (if (parts[1].equals("lovelace", ignoreCase = true) || parts[1].equals("ADA", ignoreCase = true)) "ADA" else parts[1])
                parts.size == 1 -> parts[0] to defaultQuote
                else -> throw IllegalArgumentException("Invalid pair format: $pair")
            }
        }.toSet()
    }

    protected fun parseCandleRequest(request: HistoricalCandlesRequest): Triple<String, String, Long> {
        val (asset, quote) = request.symbol.split(":").let { parts ->
            when {
                parts.size == 2 -> parts[0] to (if (parts[1].equals("ADA", ignoreCase = true)) "ADA" else parts[1])
                parts.size == 1 -> parts[0] to defaultQuote
                else -> throw IllegalArgumentException("Invalid pair format: ${request.symbol}")
            }
        }
        val dbQuote = if (quote.equals("ADA", ignoreCase = true)) "lovelace" else quote
        val period = when (request.resolution) {
            Helpers.RESO_DEFN_15M -> 900L
            Helpers.RESO_DEFN_1H -> 3600L
            Helpers.RESO_DEFN_1D -> 86400L
            Helpers.RESO_DEFN_1W -> 604800L
            else -> throw IllegalArgumentException("Unsupported resolution: ${request.resolution}")
        }
        return Triple(asset, dbQuote, period)
    }

    protected fun alignTimes(request: HistoricalCandlesRequest, period: Long, fromBase: Long): Pair<Long, Long> {
        val toTime = request.to ?: LocalDateTime.now(ZoneOffset.UTC).toEpochSecond(ZoneOffset.UTC)
        return if (request.resolution == Helpers.RESO_DEFN_1W) {
            val fromSunday = Helpers.alignToMonday(fromBase)
            val toSunday = Helpers.alignToMonday(toTime)
            fromSunday to toSunday
        } else {
            (fromBase / period * period) to (toTime / period * period)
        }
    }
}