package tech.edgx.prise.webserver.service.price

import com.fasterxml.jackson.core.type.TypeReference
import jakarta.annotation.Resource
import org.springframework.context.annotation.Profile
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Service
import tech.edgx.prise.webserver.model.prices.AssetPrice
import tech.edgx.prise.webserver.model.prices.CandleResponse
import tech.edgx.prise.webserver.model.prices.HistoricalCandlesRequest
import tech.edgx.prise.webserver.model.prices.LatestPricesRequest
import tech.edgx.prise.webserver.service.AssetService
import tech.edgx.prise.webserver.service.CandleService
import tech.edgx.prise.webserver.service.LatestPriceService
import tech.edgx.prise.webserver.util.DexEnum
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.math.max

@Service("priceService")
@Profile("cache")
class CachingPriceService(
    @Resource(name = "candleService") override val candleService: CandleService,
    @Resource(name = "assetService") override val assetService: AssetService,
    @Resource(name = "latestPriceService") override val latestPriceService: LatestPriceService,
    @Resource(name = "byteArrayRedisTemplate") private val byteRedisTemplate: RedisTemplate<String, ByteArray>,
    @Resource(name = "stringRedisTemplate") private val stringRedisTemplate: StringRedisTemplate
) : AbstractPriceService(candleService, assetService, latestPriceService) {

    override fun getAllSupportedPairs(): Set<Pair<String, String>> {
        val cacheKey = "active_pairs"
        val cachedPairs = stringRedisTemplate.opsForSet().members(cacheKey)
        if (!cachedPairs.isNullOrEmpty()) {
            return cachedPairs.map { it.split(":").let { it[0] to it[1] } }.toSet().also {
                log.debug("Retrieved {} pairs from cache: {}", it.size, it)
            }
        }

        val pairs = assetService.getAllSupportedPairs()
        log.debug("Fetched {} pairs from assetService: {}", pairs.size, pairs)
        stringRedisTemplate.opsForSet().add(cacheKey, *pairs.map { "${it.first}:${it.second}" }.toTypedArray())
        stringRedisTemplate.expire(cacheKey, 1, TimeUnit.HOURS)
        return pairs
    }

    override fun getLatestPrices(latestPricesRequest: LatestPricesRequest): List<AssetPrice> {
        val pricingProviderNames = DexEnum.entries.map { it.friendlyName }.toSet()
        val pricingProviderIds = DexEnum.entries.map { it.code }.toSet()
        val cacheKeyPrefix = "latest_prices"
        val cacheKeyAll = "${cacheKeyPrefix}_all"
        val cachedPrices = mutableListOf<AssetPrice>()
        val parsedPairs = parsePairs(latestPricesRequest)
        log.debug("Parsed pairs: {}", parsedPairs)

        if (parsedPairs.isEmpty()) {
            val cachedBytes = byteRedisTemplate.opsForValue().get(cacheKeyAll)
            if (cachedBytes != null) {
                try {
                    val json = decompress(cachedBytes)
                    val prices = objectMapper.readValue(json, object : TypeReference<List<AssetPrice>>() {})
                    cachedPrices.addAll(prices)
                    log.debug("Retrieved {} prices from all cache: {}", prices.size, cacheKeyAll)
                    return prices.filter { it.provider != null && it.provider in pricingProviderNames }
                        .also { log.debug("Returning {} prices", it.size) }
                } catch (e: Exception) {
                    log.warn("Failed to decompress or deserialize cached prices for key: $cacheKeyAll", e)
                }
            }

            val freshPrices = latestPriceService.getLatestPrices(emptySet(), pricingProviderIds)
            log.debug("Fetched fresh prices: {}", freshPrices.size)
            if (freshPrices.isEmpty()) {
                log.warn("No prices returned from latestPriceService for all pairs")
            } else {
                try {
                    val json = objectMapper.writeValueAsString(freshPrices)
                    byteRedisTemplate.opsForValue().set(
                        cacheKeyAll,
                        compress(json),
                        20, TimeUnit.SECONDS
                    )
                    log.debug("Cached all prices for key: {}, size: {}", cacheKeyAll, json.length)
                } catch (e: Exception) {
                    log.error("Failed to cache all prices for key: $cacheKeyAll", e)
                }
            }
            cachedPrices.addAll(freshPrices)
        } else {
            val missingPairs = mutableSetOf<Pair<String, String>>()
            val keys = parsedPairs.map { pair ->
                val dbQuote = if (pair.second.equals("ADA", ignoreCase = true)) "lovelace" else pair.second
                "${cacheKeyPrefix}:${pair.first}:${dbQuote}"
            }
            log.debug("Fetching keys: {}", keys)

            byteRedisTemplate.opsForValue().multiGet(keys)?.forEachIndexed { i, cachedBytes ->
                if (cachedBytes != null) {
                    try {
                        val json = decompress(cachedBytes)
                        val prices = objectMapper.readValue(json, object : TypeReference<List<AssetPrice>>() {})
                        cachedPrices.addAll(prices)
                        log.debug("Retrieved prices for key: {}, count: {}", keys[i], prices.size)
                    } catch (e: Exception) {
                        log.warn("Failed to decompress or deserialize cached prices for key: ${keys[i]}", e)
                        missingPairs.add(parsedPairs.elementAt(i))
                    }
                } else {
                    missingPairs.add(parsedPairs.elementAt(i))
                }
            }
            log.debug("Cached prices found: {}, missing pairs: {}", cachedPrices.size, missingPairs.size)

            if (missingPairs.isNotEmpty()) {
                log.debug("Fetching missing pairs: {}", missingPairs)
                try {
                    val freshPrices = latestPriceService.getLatestPrices(missingPairs, pricingProviderIds)
                    log.debug("Fetched fresh prices: {}", freshPrices.size)
                    if (freshPrices.isEmpty()) {
                        log.warn("No prices returned from latestPriceService for {} pairs", missingPairs.size)
                    }
                    freshPrices.groupBy { it.asset to it.quote }.forEach { (pair, prices) ->
                        val dbQuote = if (pair.second.equals("ADA", ignoreCase = true)) "lovelace" else pair.second
                        val cacheKey = "${cacheKeyPrefix}:${pair.first}:${dbQuote}"
                        try {
                            val json = objectMapper.writeValueAsString(prices)
                            byteRedisTemplate.opsForValue().set(
                                cacheKey,
                                compress(json),
                                10, TimeUnit.SECONDS
                            )
                            log.debug("Cached prices for key: {}, size: {}, prices: {}", cacheKey, json.length, prices)
                        } catch (e: Exception) {
                            log.error("Failed to cache prices for key: {}", cacheKey, e)
                        }
                    }
                    cachedPrices.addAll(freshPrices)
                } catch (e: Exception) {
                    log.error("Failed to fetch prices from latestPriceService for pairs: {}", missingPairs, e)
                }
            }
        }

        return cachedPrices
            .filter { it.provider != null && it.provider in pricingProviderNames }
            .also { log.debug("Returning {} prices", it.size) }
    }

    override fun getCandles(request: HistoricalCandlesRequest): List<CandleResponse> {
        log.debug("Submitted form: {}", request)
        val (asset, dbQuote, period) = parseCandleRequest(request)
        val assetId = assetService.getAssetIdForUnit(asset) ?: throw IllegalArgumentException("Unknown asset: $asset")
        val quoteAssetId = assetService.getAssetIdForUnit(dbQuote) ?: throw IllegalArgumentException("Unknown quote: $dbQuote")
        val toTime = request.to ?: LocalDateTime.now(ZoneOffset.UTC).toEpochSecond(ZoneOffset.UTC)
        val fromTime = request.from ?: dexLaunchTime
        log.debug("Time range: from=$fromTime, to=$toTime")

        val cacheKeyPrefix = "candles:$assetId:$quoteAssetId:${request.resolution}"
        val minTimeCacheKey = "$cacheKeyPrefix:minTime"
        var cachedFirstDbTime = stringRedisTemplate.opsForValue().get(minTimeCacheKey)?.toLongOrNull()
        val fromBase = cachedFirstDbTime ?: fromTime
        val (alignedFromTime, alignedToTime) = alignTimes(request, period, fromBase)

        val cachedCandles = mutableMapOf<Long, CandleResponse>()
        val times = generateSequence(alignedFromTime) { it + period }
            .takeWhile { it <= alignedToTime }
            .toList()
        log.debug("Cache lookup times: min={}, max={}", times.minOrNull(), times.maxOrNull())
        times.chunked(100).forEach { chunk ->
            val keys = chunk.map { "$cacheKeyPrefix:$it" }
            byteRedisTemplate.opsForValue().multiGet(keys)?.forEachIndexed { i, cachedBytes ->
                if (cachedBytes != null) {
                    try {
                        val json = decompress(cachedBytes)
                        val candle = objectMapper.readValue(json, object : TypeReference<CandleResponse>() {})
                        cachedCandles[candle.time!!] = candle
                    } catch (e: Exception) {
                        log.warn("Failed to decompress or deserialize candle: ${keys[i]}", e)
                    }
                }
            }
        }
        log.debug("Cached candles found: {}", cachedCandles.size)

        val earliestCachedTime = if (cachedCandles.isNotEmpty()) cachedCandles.keys.min() else null
        val latestCachedCandle = if (cachedCandles.isNotEmpty()) cachedCandles[cachedCandles.keys.max()] else null
        val latestCachedTime = latestCachedCandle?.time
        log.debug("Cached candles found, earliest: {}, latest: {}", earliestCachedTime, latestCachedTime)
        if (cachedCandles.isNotEmpty() && earliestCachedTime != null && latestCachedTime != null) {
            val effectiveFromTime = if (request.from == null && cachedFirstDbTime != null) {
                max(alignedFromTime, cachedFirstDbTime)
            } else {
                alignedFromTime
            }
            if (earliestCachedTime <= effectiveFromTime && latestCachedTime + period >= toTime) {
                log.debug("Cache covers range, skipping DB query")
                return cachedCandles.values
                    .sortedBy { it.time }
                    .filter { it.time in fromTime..toTime }
                    .also { log.debug("Returning {} candles", it.size) }
            }
        }
        log.debug("From: {}, earliestCachedTime: {}, latestCachedTime: {}", alignedFromTime, earliestCachedTime, latestCachedTime)

        if (request.from == null && cachedFirstDbTime == null) {
            cachedFirstDbTime = candleService.getFirstCandleTime(assetId, quoteAssetId, request.resolution, toTime)
            if (cachedFirstDbTime != null) {
                try {
                    stringRedisTemplate.opsForValue().set(
                        minTimeCacheKey,
                        cachedFirstDbTime.toString(),
                        24L * 60,
                        TimeUnit.MINUTES
                    )
                    log.debug("Cached firstDbTime: {} for key: {}", cachedFirstDbTime, minTimeCacheKey)
                } catch (e: Exception) {
                    log.warn("Failed to cache firstDbTime for key: {}", minTimeCacheKey, e)
                }
            }
        }

        val dbFromTime = if (cachedCandles.isNotEmpty()) {
            if (alignedFromTime >= earliestCachedTime!!) {
                log.debug("Querying newer candles from {}", latestCachedTime?.plus(period))
                latestCachedTime?.plus(period)
            } else {
                if (request.from != null) {
                    val latestDbTime = candleService.getLatestCandleTime(assetId, quoteAssetId, request.resolution, alignedFromTime)
                    log.debug("Querying from maxNotNull of latestDbTime prior to fromTime, latestDbTime: {} And known firstDbTime: {}", latestDbTime, cachedFirstDbTime)
                    listOfNotNull(alignedFromTime, latestDbTime, cachedFirstDbTime).max()
                } else {
                    if (earliestCachedTime == cachedFirstDbTime!!) {
                        log.debug("Querying from next period after latestCachedTime: {}", latestCachedTime?.plus(period))
                        latestCachedTime?.plus(period)
                    } else {
                        log.debug("Querying from first DB Candle Time: {}", cachedFirstDbTime)
                        cachedFirstDbTime
                    }
                }
            }
        } else {
            val latestDbTime = candleService.getLatestCandleTime(assetId, quoteAssetId, request.resolution, alignedFromTime)
            log.debug("Querying from latestDbTime prior to fromTime: {}, latestDbTime: {}", alignedFromTime, latestDbTime)
            listOfNotNull(latestDbTime, alignedFromTime).min()
        }
        log.debug("Querying DB from {} to {}", dbFromTime, toTime)

        if (dbFromTime == null) {
            when (cachedCandles.isNotEmpty()) {
                true -> {
                    log.debug("No candles prior to requested fromTime and no candles before cache in DB, returning cache as-is")
                    return cachedCandles.values.toList()
                }
                false -> {
                    log.debug("No prior candles in DB or Cache, returning empty list")
                    return emptyList()
                }
            }
        }

        val dbCandles = candleService.getCandles(
            assetId,
            quoteAssetId,
            request.resolution,
            dbFromTime,
            toTime,
            latestCachedCandle
        )
        log.debug("# Db candles: {}, latest: {}", dbCandles.size, dbCandles.last())

        if (request.from == null && cachedCandles.isEmpty() && cachedFirstDbTime == null) {
            val firstDbTime = dbCandles.minByOrNull { it.time!! }?.time
            if (firstDbTime != null) {
                try {
                    stringRedisTemplate.opsForValue().set(
                        minTimeCacheKey,
                        firstDbTime.toString(),
                        24L * 60,
                        TimeUnit.MINUTES
                    )
                    log.debug("Cached firstDbTime: {} for key: {}", firstDbTime, minTimeCacheKey)
                } catch (e: Exception) {
                    log.warn("Failed to cache firstDbTime for key: {}", minTimeCacheKey, e)
                }
            }
        }

        dbCandles.forEach { candle ->
            if (candle.time != null) {
                cachedCandles[candle.time] = candle
                try {
                    val json = objectMapper.writeValueAsString(candle)
                    val ttl = if (candle.time >= alignedToTime) 30L else 24L * 60 * 60
                    byteRedisTemplate.opsForValue().set(
                        "$cacheKeyPrefix:${candle.time}",
                        compress(json),
                        ttl,
                        TimeUnit.SECONDS
                    )
                } catch (e: Exception) {
                    log.warn("Failed to cache candle for key: $cacheKeyPrefix:${candle.time}", e)
                }
            }
        }
        log.debug("Updated cachedCandles found: {}, min value: {}, max value: {}", cachedCandles.values.size, cachedCandles.keys.min(), cachedCandles.keys.max())

        return cachedCandles.values
            .sortedBy { it.time }
            .filter { it.time in fromTime..toTime }
            .also { log.debug("Returning {} candles", it.size) }
    }

    private fun compress(data: String): ByteArray {
        val bos = ByteArrayOutputStream()
        GZIPOutputStream(bos).use { gos ->
            gos.write(data.toByteArray(Charsets.UTF_8))
        }
        return bos.toByteArray()
    }

    private fun decompress(data: ByteArray): String {
        val bis = ByteArrayInputStream(data)
        GZIPInputStream(bis).use { gis ->
            return String(gis.readAllBytes(), Charsets.UTF_8)
        }
    }
}