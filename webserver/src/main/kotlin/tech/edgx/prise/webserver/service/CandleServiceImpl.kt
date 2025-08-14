package tech.edgx.prise.webserver.service

import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Service
import tech.edgx.prise.webserver.model.prices.CandleResponse
import tech.edgx.prise.webserver.util.Helpers
import java.sql.SQLException

interface CandleService {
    fun getCandles(assetId: Long, quoteAssetId: Long, resolution: String, from: Long, to: Long, lastCandle: CandleResponse?): List<CandleResponse>
    fun getLatestCandleTime(assetId: Long, quoteAssetId: Long, resolution: String, maxTime: Long): Long?
    fun getFirstCandleTime(assetId: Long, quoteAssetId: Long, resolution: String, maxTime: Long): Long?
}

@Service("candleService")
class CandleServiceImpl(
    private val jdbcClient: JdbcClient
) : CandleService {
    private val log = LoggerFactory.getLogger(CandleServiceImpl::class.java)

    override fun getCandles(assetId: Long, quoteAssetId: Long, resolution: String, from: Long, to: Long, latestCandle: CandleResponse?): List<CandleResponse> {
        try {
            val period = when (resolution) {
                Helpers.RESO_DEFN_15M -> 900L
                Helpers.RESO_DEFN_1H -> 3600L
                Helpers.RESO_DEFN_1D -> 86400L
                Helpers.RESO_DEFN_1W -> 604800L
                else -> throw IllegalArgumentException("Unsupported resolution: $resolution")
            }

            val candles = queryMaterializedView(assetId, quoteAssetId, resolution, from, to)
            // if candles is empty, put in lastCandle (which comes from cache)
            log.debug("Latest candle: {}", latestCandle)
            val candlesToInterpolate = candles.ifEmpty { latestCandle?.let { listOf(it) } ?: emptyList() }
            log.debug("Candles to interpolate: {}, last: {}", candlesToInterpolate.size, candlesToInterpolate.last())
            val adjustedFrom = candlesToInterpolate.mapNotNull { it.time }.minOrNull() ?: from

            val finalCandles = interpolateCandles(candlesToInterpolate, period, adjustedFrom, to)
            log.debug("# historical candles: {}, after interpolation: {}", candles.size, finalCandles.size)
            return finalCandles
        } catch (e: SQLException) {
            log.error("Error querying candles for assetId=$assetId, quoteAssetId=$quoteAssetId, resolution=$resolution", e)
            return emptyList()
        }
    }

    private fun queryMaterializedView(assetId: Long, quoteAssetId: Long, resolution: String, from: Long, to: Long): List<CandleResponse> {
        val viewName = when (resolution) {
            Helpers.RESO_DEFN_1W -> "candle_weekly"
            Helpers.RESO_DEFN_1D -> "candle_daily"
            Helpers.RESO_DEFN_1H -> "candle_hourly"
            Helpers.RESO_DEFN_15M -> "candle_fifteen"
            else -> return emptyList()
        }
        log.debug("Querying {} {} {} {} {}", viewName, assetId, resolution, from, to)
        return jdbcClient
            .sql("""
                SELECT time, open, high, low, close, volume
                FROM $viewName
                WHERE asset_id = :asset_id
                AND quote_asset_id = :quote_asset_id
                AND time BETWEEN :from AND :to
                ORDER BY time
            """.trimIndent())
            .param("asset_id", assetId)
            .param("quote_asset_id", quoteAssetId)
            .param("from", from)
            .param("to", to)
            .query { rs, _ ->
                CandleResponse(
                    time = rs.getLong("time"),
                    open = rs.getDouble("open"),
                    high = rs.getDouble("high"),
                    low = rs.getDouble("low"),
                    close = rs.getDouble("close"),
                    volume = rs.getDouble("volume")
                )
            }
            .list()
    }

    private fun interpolateCandles(candles: List<CandleResponse>, period: Long, from: Long, to: Long): List<CandleResponse> {
        val result = mutableListOf<CandleResponse>()
        val candleMap = candles.associateBy { it.time!! }.toSortedMap()
        var lastClose: Double? = null
        val isWeekly = period == 604800L
        // Align to Monday for weekly, otherwise use period alignment
        val baseAlignedFrom = if (isWeekly) Helpers.alignToMonday(from) else from / period * period
        // Use the earliest candle time if available, otherwise fall back to baseAlignedFrom
        val earliestCandleTime = candleMap.keys.minOrNull() ?: from
        val alignedFrom = maxOf(baseAlignedFrom, earliestCandleTime)
        var currentTime = alignedFrom

        // If no candles and no lastClose, return empty list
        if (candleMap.isEmpty() && lastClose == null) {
            log.debug("No candles or lastClose for range {} to {}", from, to)
            return result
        }

        // Iterate over periods using iterator for efficient lookup
        val candleIterator = candleMap.entries.iterator()
        var nextCandle: MutableMap.MutableEntry<Long, CandleResponse>? = if (candleIterator.hasNext()) candleIterator.next() else null

        while (currentTime <= to) {
            // Select candle if its time falls within the current period
            val candle = if (nextCandle != null && nextCandle.key >= currentTime && nextCandle.key < currentTime + period) {
                nextCandle.value
            } else {
                null
            }

            if (candle != null && candle.close != null) {
                // Use DB candle, align time to period start
                val alignedCandle = candle.copy(time = currentTime)
                result.add(alignedCandle)
                lastClose = candle.close
                log.trace("Added DB candle at {} with close={}", currentTime, lastClose)
                nextCandle = if (candleIterator.hasNext()) candleIterator.next() else null
            } else if (lastClose != null) {
                // Generate zero-volume candle
                val interpolatedCandle = CandleResponse(
                    time = currentTime,
                    open = lastClose,
                    high = lastClose,
                    low = lastClose,
                    close = lastClose,
                    volume = 0.0
                )
                result.add(interpolatedCandle)
                log.trace("Interpolated candle at {} with close={}", currentTime, lastClose)
            }
            currentTime += period
        }

        if (result.isEmpty()) {
            log.debug("No candles generated for range {} to {}; lastClose={}", from, to, lastClose)
        }
        return result
    }

    override fun getLatestCandleTime(assetId: Long, quoteAssetId: Long, resolution: String, maxTime: Long): Long? {
        val viewName = when (resolution) {
            Helpers.RESO_DEFN_1W -> "candle_weekly"
            Helpers.RESO_DEFN_1D -> "candle_daily"
            Helpers.RESO_DEFN_1H -> "candle_hourly"
            Helpers.RESO_DEFN_15M -> "candle_fifteen"
            else -> return null
        }
        log.debug("Querying latest candle time for assetId={}, quoteAssetId={}, resolution={}, maxTime={}",
            assetId, quoteAssetId, resolution, maxTime)
        return jdbcClient
            .sql("""
                SELECT MAX(time)
                FROM $viewName
                WHERE asset_id = :asset_id
                AND quote_asset_id = :quote_asset_id
                AND time <= :max_time
            """.trimIndent()) //
            .param("asset_id", assetId)
            .param("quote_asset_id", quoteAssetId)
            .param("max_time", maxTime)
            .query(Long::class.java)
            .optional()
            .orElse(null)
    }

    override fun getFirstCandleTime(assetId: Long, quoteAssetId: Long, resolution: String, maxTime: Long): Long? {
        val viewName = when (resolution) {
            Helpers.RESO_DEFN_1W -> "candle_weekly"
            Helpers.RESO_DEFN_1D -> "candle_daily"
            Helpers.RESO_DEFN_1H -> "candle_hourly"
            Helpers.RESO_DEFN_15M -> "candle_fifteen"
            else -> return null
        }
        log.debug("Querying first candle time for assetId={}, quoteAssetId={}, resolution={}, maxTime={}",
            assetId, quoteAssetId, resolution, maxTime)
        return jdbcClient
            .sql("""
                SELECT MIN(time)
                FROM $viewName
                WHERE asset_id = :asset_id
                AND quote_asset_id = :quote_asset_id
                AND time <= :max_time
            """.trimIndent())
            .param("asset_id", assetId)
            .param("quote_asset_id", quoteAssetId)
            .param("max_time", maxTime)
            .query(Long::class.java)
            .optional()
            .orElse(null)
    }
}