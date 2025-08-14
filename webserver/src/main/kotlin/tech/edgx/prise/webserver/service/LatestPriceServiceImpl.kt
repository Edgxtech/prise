package tech.edgx.prise.webserver.service

import jakarta.annotation.Resource
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Service
import tech.edgx.prise.webserver.model.prices.AssetPrice
import tech.edgx.prise.webserver.util.DexEnum
import java.sql.SQLException

interface LatestPriceService {
    fun getLatestPrices(assetIds: Set<Pair<String, String>>, providerIds: Set<Int>): List<AssetPrice>
    fun getLatestPricesByIds(assetIds: Set<Pair<Long, Long>>, providerIds: Set<Int>): List<AssetPrice>
}

@Service("latestPriceService")
class LatestPriceServiceImpl(
    @Resource(name = "assetService") private val assetService: AssetService,
    private val jdbcClient: JdbcClient
) : LatestPriceService {
    private val log = LoggerFactory.getLogger(LatestPriceServiceImpl::class.java)

    override fun getLatestPricesByIds(assetIds: Set<Pair<Long, Long>>, providerIds: Set<Int>): List<AssetPrice> {
        val batchSize = 100
        val allPrices = mutableListOf<AssetPrice>()

        if (assetIds.isEmpty()) {
            // Fetch all pairs
            val sql = """
                SELECT 
                    lp.asset_id,
                    lp.quote_asset_id,
                    a2.unit AS asset,
                    a1.unit AS quote,
                    a2.native_name AS name,
                    lp.price,
                    lp.time,
                    lp.provider
                FROM latest_price lp
                JOIN asset a1 ON a1.id = lp.quote_asset_id
                JOIN asset a2 ON a2.id = lp.asset_id
                WHERE lp.provider IN (:providers)
            """.trimIndent()
            log.debug("Querying: {}", sql)
            try {
                val prices = jdbcClient
                    .sql(sql)
                    .param("providers", providerIds.toList())
                    .query { rs, _ ->
                        AssetPrice(
                            asset = rs.getString("asset"),
                            quote = if (rs.getString("quote") == "lovelace") "ADA" else rs.getString("quote"),
                            asset_id = rs.getLong("asset_id"),
                            quote_asset_id = rs.getLong("quote_asset_id"),
                            name = rs.getString("name") ?: rs.getString("asset"),
                            last_price = rs.getDouble("price").takeIf { !rs.wasNull() },
                            last_update = rs.getLong("time").takeIf { !rs.wasNull() },
                            provider = DexEnum.entries.find { it.code == rs.getInt("provider") }?.friendlyName
                        )
                    }
                    .list()
                allPrices.addAll(prices)
                log.debug("Fetched {} prices for all pairs", prices.size)
            } catch (e: Exception) {
                log.error("Failed to fetch prices for all pairs", e)
            }
        } else {
            // Batch pairs
            val batches = assetIds.chunked(batchSize)
            batches.forEachIndexed { batchIndex, batch ->
                // Create VALUES clause
                val valuesClause = batch.mapIndexed { i, pair -> "(:asset_id_$i, :quote_asset_id_$i)" }.joinToString(", ")
                val sql = """
                SELECT 
                    lp.asset_id,
                    lp.quote_asset_id,
                    a2.unit AS asset,
                    a1.unit AS quote,
                    a2.native_name AS name,
                    lp.price,
                    lp.time,
                    lp.provider
                FROM latest_price lp
                JOIN asset a1 ON a1.id = lp.quote_asset_id
                JOIN asset a2 ON a2.id = lp.asset_id
                WHERE lp.provider IN (:providers)
                AND (lp.asset_id, lp.quote_asset_id) IN ($valuesClause)
            """.trimIndent()

                try {
                    val prices = jdbcClient
                        .sql(sql)
                        .param("providers", providerIds.toList())
                        .also { query ->
                            batch.forEachIndexed { i, pair ->
                                query.param("asset_id_$i", pair.first)
                                query.param("quote_asset_id_$i", pair.second)
                            }
                        }
                        .query { rs, _ ->
                            AssetPrice(
                                asset = rs.getString("asset"),
                                quote = if (rs.getString("quote") == "lovelace") "ADA" else rs.getString("quote"),
                                asset_id = rs.getLong("asset_id"),
                                quote_asset_id = rs.getLong("quote_asset_id"),
                                name = rs.getString("name") ?: rs.getString("asset"),
                                last_price = rs.getDouble("price").takeIf { !rs.wasNull() },
                                last_update = rs.getLong("time").takeIf { !rs.wasNull() },
                                provider = DexEnum.entries.find { it.code == rs.getInt("provider") }?.friendlyName
                            )
                        }
                        .list()
                    allPrices.addAll(prices)
                    log.debug("Fetched {} prices for batch {}", prices.size, batchIndex)
                } catch (e: Exception) {
                    log.error("Failed to fetch prices for batch {}: {}", batchIndex, batch, e)
                }
            }
        }

        return allPrices
    }

    override fun getLatestPrices(pairs: Set<Pair<String, String>>, pricingProviders: Set<Int>): List<AssetPrice> {
        val sql = if (pairs.isEmpty()) {
            """
            SELECT
                a2.unit AS asset,
                a1.unit AS quote,
                lp.asset_id,
                lp.quote_asset_id,
                a2.native_name AS name,
                lp.price,
                lp.time,
                lp.provider
            FROM latest_price lp
            JOIN asset a2 ON a2.id = lp.asset_id
            JOIN asset a1 ON a1.id = lp.quote_asset_id
            WHERE lp.provider IN (:pricing_providers)
            """.trimIndent()
        } else {
            val pairsTuple = pairs.joinToString(",") { (asset, quote) ->
                val dbQuote = if (quote.equals("ADA", ignoreCase = true)) "lovelace" else quote
                "('$asset','$dbQuote')"
            }
            log.debug("Fetching for db pairs: {}", pairsTuple)
            """
            SELECT
                a2.unit AS asset,
                a1.unit AS quote,
                lp.asset_id,
                lp.quote_asset_id,
                a2.native_name AS name,
                lp.price,
                lp.time,
                lp.provider
            FROM latest_price lp
            JOIN asset a2 ON a2.id = lp.asset_id
            JOIN asset a1 ON a1.id = lp.quote_asset_id
            WHERE lp.provider IN (:pricing_providers)
            AND (a2.unit, a1.unit) IN ($pairsTuple)
            """.trimIndent()
        }

        return try {
            log.debug("SQL Query: {}", sql)
            jdbcClient.sql(sql)
                .param("pricing_providers", pricingProviders.toList())
                .query { rs, _ ->
                    AssetPrice(
                        asset = rs.getString("asset"),
                        quote = if (rs.getString("quote") == "lovelace") "ADA" else rs.getString("quote"),
                        asset_id = rs.getLong("asset_id"),
                        quote_asset_id = rs.getLong("quote_asset_id"),
                        name = rs.getString("name") ?: rs.getString("asset"),
                        last_price = rs.getDouble("price").takeIf { !rs.wasNull() },
                        last_update = rs.getLong("time").takeIf { !rs.wasNull() },
                        provider = DexEnum.entries.find { it.code == rs.getInt("provider") }?.friendlyName
                    )
                }.list()
        } catch (e: SQLException) {
            log.error("Error querying prices for pairs: ${pairs}", e)
            emptyList()
        }
    }
}
