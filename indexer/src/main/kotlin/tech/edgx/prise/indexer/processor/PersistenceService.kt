package tech.edgx.prise.indexer.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.ktorm.database.Database
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.domain.Price
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.PriceService
import tech.edgx.prise.indexer.service.TxService
import tech.edgx.prise.indexer.util.Helpers
import java.sql.Types
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class PersistenceService(private val config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(this::class.java)
    private val assetService: AssetService by inject { parametersOf(config) }
    private val txService: TxService by inject { parametersOf(config) }
    private val priceService: PriceService by inject { parametersOf(config) }
    private val database: Database by inject(named("appDatabase"))
    private val refreshViewLocks = ConcurrentHashMap<String, ReentrantLock>()
    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    fun persistAssets(assets: List<Asset>) {
        if (assets.isEmpty()) return
        assetService.batchInsert(assets.groupBy { it.unit }.map { it.value.first() })
        log.info("Persisted # assets: {}", assets.size)
    }

    fun persistTransactions(txHashes: List<String>) {
        if (txHashes.isEmpty()) return
        txService.batchInsertTxs(txHashes.map { Helpers.hexToBinary(it) })
        log.debug("Persisted # transactions: {}", txHashes.size)
    }

    fun persistPrices(prices: List<Price>) {
        priceService.batchInsertOrUpdate(prices)
        log.debug("Persisted # prices: {}", prices.size)
    }

    fun persistLatestPrices(prices: List<Price>) {
        priceService.batchInsertOrUpdateLatest(prices)
        log.debug("Persisted # latest prices: {}", prices.size)
    }

    fun persistPricesCombined(prices: List<Price>) {
        priceService.batchInsertOrUpdateCombined(prices)
        log.debug("Persisted # prices: {}", prices.size)
    }

    // Attempt to persist in parallel - creates other dramas w/connection pool etc.. leave it out for now
    suspend fun asyncPersistPrices(prices: List<Price>) = withContext(Dispatchers.IO) {
        if (prices.isEmpty()) return@withContext
        coroutineScope {
            launch { priceService.asyncBatchInsertOrUpdate(prices) }
            launch { priceService.asyncBatchInsertOrUpdateLatest(prices)}
        }
        log.debug("Persisted # prices and latest prices: {}", prices.size)
    }

    suspend fun refreshViews(newPrices: List<Price>?) {
        val jsonPrices = newPrices?.takeIf { it.isNotEmpty() }?.let { prices ->
            objectMapper.writeValueAsString(prices.map { price ->
                mapOf(
                    "asset_id" to price.asset_id,
                    "quote_asset_id" to price.quote_asset_id,
                    "time" to price.time,
                    "tx_id" to price.tx_id,
                    "tx_swap_idx" to price.tx_swap_idx,
                    "price" to price.price,
                    "amount1" to price.amount1,
                    "outlier" to price.outlier
                )
            })
        }
        log.debug("Refreshing # views: {} with new prices: #: {}, {}", config.refreshableViews.size, newPrices?.size ?: 0, jsonPrices)

        config.refreshableViews.forEach {
            refreshView(it.key, jsonPrices)
        }
    }

    suspend fun refreshView(viewName: String, jsonPrices: String? = null) {
        withContext(Dispatchers.IO) {
            refreshViewLocks.computeIfAbsent(viewName) { ReentrantLock() }.withLock {
                val startTime = System.currentTimeMillis()
                try {
                    database.useConnection { conn ->
                        val query = if (jsonPrices != null) {
                            "CALL refresh_${viewName}_incremental(specific_prices_json := ?)"
                        } else {
                            "CALL refresh_${viewName}_incremental()"
                        }
                        conn.prepareStatement(query).use { stmt ->
                            if (jsonPrices != null) {
                                stmt.setObject(1, jsonPrices, Types.OTHER)
                            }
                            log.debug("Refreshing view: {}, query: {}", stmt, query)
                            stmt.execute()
                        }
                    }
                    log.debug("Refreshed view {} in {}ms", viewName, System.currentTimeMillis() - startTime)
                } catch (e: Exception) {
                    log.error("Failed to refresh view $viewName, prices: $jsonPrices", e)
                    throw e
                }
            }
        }
    }
}