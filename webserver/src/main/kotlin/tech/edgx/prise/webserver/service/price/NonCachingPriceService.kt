package tech.edgx.prise.webserver.service.price

import jakarta.annotation.Resource
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import tech.edgx.prise.webserver.model.prices.AssetPrice
import tech.edgx.prise.webserver.model.prices.CandleResponse
import tech.edgx.prise.webserver.model.prices.HistoricalCandlesRequest
import tech.edgx.prise.webserver.model.prices.LatestPricesRequest
import tech.edgx.prise.webserver.service.AssetService
import tech.edgx.prise.webserver.service.CandleService
import tech.edgx.prise.webserver.service.LatestPriceService
import tech.edgx.prise.webserver.util.DexEnum
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service("priceService")
@Profile("!cache")
class NonCachingPriceService(
    @Resource(name = "candleService") override val candleService: CandleService,
    @Resource(name = "assetService") override val assetService: AssetService,
    @Resource(name = "latestPriceService") override val latestPriceService: LatestPriceService,
) : AbstractPriceService(candleService, assetService, latestPriceService) {

    override fun getAllSupportedPairs(): Set<Pair<String, String>> {
        log.debug("Fetching all supported pairs from assetService")
        return assetService.getAllSupportedPairs()
    }

    override fun getLatestPrices(latestPricesRequest: LatestPricesRequest): List<AssetPrice> {
        val pricingProviderNames = DexEnum.entries.map { it.friendlyName }.toSet()
        val pricingProviderIds = DexEnum.entries.map { it.code }.toSet()
        val parsedPairs = parsePairs(latestPricesRequest)
        log.debug("Parsed pairs: {}", parsedPairs)

        val freshPrices = latestPriceService.getLatestPrices(parsedPairs, pricingProviderIds)
        log.debug("Fetched fresh prices: {}", freshPrices.size)
        if (freshPrices.isEmpty()) {
            log.warn("No prices returned from latestPriceService for {} pairs", parsedPairs.size)
        }

        return freshPrices.filter { it.provider != null && it.provider in pricingProviderNames }
            .also { log.debug("Returning {} prices", it.size) }
    }

    override fun getCandles(historicalCandlesRequest: HistoricalCandlesRequest): List<CandleResponse> {
        val (asset, dbQuote, period) = parseCandleRequest(historicalCandlesRequest)
        val assetId = assetService.getAssetIdForUnit(asset) ?: throw IllegalArgumentException("Unknown asset: $asset")
        val quoteAssetId = assetService.getAssetIdForUnit(dbQuote) ?: throw IllegalArgumentException("Unknown quote: $dbQuote")
        val toTime = historicalCandlesRequest.to ?: LocalDateTime.now(ZoneOffset.UTC).toEpochSecond(ZoneOffset.UTC)
        val fromTime = historicalCandlesRequest.from ?: dexLaunchTime
        log.debug("Time range: from={}, to={}", fromTime, toTime)

        val (alignedFromTime, alignedToTime) = alignTimes(historicalCandlesRequest, period, fromTime)
        val latestDbTime = candleService.getLatestCandleTime(assetId, quoteAssetId, historicalCandlesRequest.resolution, alignedFromTime)
        val dbFromTime = listOfNotNull(latestDbTime, alignedFromTime).min()
        log.debug("Querying DB from {} to {}", dbFromTime, toTime)

        val dbCandles = candleService.getCandles(
            assetId,
            quoteAssetId,
            historicalCandlesRequest.resolution,
            dbFromTime,
            toTime,
            null
        )
        log.debug("# Db candles: {}, latest: {}", dbCandles.size, dbCandles.lastOrNull())

        return dbCandles
            .sortedBy { it.time }
            .filter { it.time in fromTime..toTime }
            .also { log.debug("Returning {} candles", it.size) }
    }
}