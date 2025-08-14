package tech.edgx.prise.indexer.service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.withContext
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import org.ktorm.database.Database
import org.ktorm.dsl.*
import org.ktorm.entity.sequenceOf
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Price
import tech.edgx.prise.indexer.domain.Prices
import java.sql.SQLException
import java.sql.Types

data class PriceRecord(
    val time: Long,
    val txId: Long,
    val txSwapIdx: Int,
    val price: Double
)

class PriceService : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    private val database: Database by inject(named("appDatabase"))
    private val batchSize = 500

    val Database.prices get() = this.sequenceOf(Prices)

    fun batchInsertOrUpdate(prices: List<Price>): Int {
        var total = 0
        database.useTransaction {
            prices.chunked(batchSize).forEach { cp ->
                total += database.useConnection { conn ->
                    val sql = """
                        INSERT INTO price (
                            asset_id, quote_asset_id, provider, time, outlier, 
                            tx_id, tx_swap_idx, price, amount1, amount2, operation
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (time, tx_id, tx_swap_idx) DO UPDATE
                        SET asset_id = EXCLUDED.asset_id,
                            quote_asset_id = EXCLUDED.quote_asset_id,
                            provider = EXCLUDED.provider,
                            price = EXCLUDED.price,
                            amount1 = EXCLUDED.amount1,
                            amount2 = EXCLUDED.amount2,
                            operation = EXCLUDED.operation,
                            outlier = EXCLUDED.outlier
                    """.trimIndent()
                    conn.prepareStatement(sql).use { stmt ->
                        cp.forEach { p ->
                            stmt.setLong(1, p.asset_id)
                            stmt.setLong(2, p.quote_asset_id)
                            stmt.setInt(3, p.provider)
                            stmt.setLong(4, p.time)
                            if (p.outlier != null) {
                                stmt.setBoolean(5, p.outlier!!)
                            } else {
                                stmt.setNull(5, Types.BOOLEAN)
                            }
                            stmt.setLong(6, p.tx_id)
                            stmt.setInt(7, p.tx_swap_idx)
                            stmt.setFloat(8, p.price)
                            stmt.setBigDecimal(9, p.amount1)
                            stmt.setBigDecimal(10, p.amount2)
                            stmt.setInt(11, p.operation)
                            stmt.addBatch()
                        }
                        stmt.executeBatch().sum()
                    }
                }
                log.debug("Batch upserted prices: chunk size: {}, units: {}", cp.size, cp.map { it.time })
            }
        }
        log.debug("Total prices inserted/updated: {}", total)
        return total
    }

    fun batchInsertOrUpdateLatest(prices: List<Price>): Int {
        var total = 0
        database.useTransaction {
            prices.chunked(batchSize).forEach { cp ->
                total += database.useConnection { conn ->
                    val sql = """
                        INSERT INTO latest_price (
                            asset_id, quote_asset_id, provider, time, 
                            tx_id, price, amount1, amount2, operation
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (asset_id, quote_asset_id, provider) DO UPDATE
                        SET time = EXCLUDED.time,
                            tx_id = EXCLUDED.tx_id,
                            price = EXCLUDED.price,                          
                            amount1 = EXCLUDED.amount1,
                            amount2 = EXCLUDED.amount2,
                            operation = EXCLUDED.operation
                    """.trimIndent()
                    conn.prepareStatement(sql).use { stmt ->
                        cp.forEach { p ->
                            stmt.setLong(1, p.asset_id)
                            stmt.setLong(2, p.quote_asset_id)
                            stmt.setInt(3, p.provider)
                            stmt.setLong(4, p.time)
                            stmt.setLong(5, p.tx_id)
                            stmt.setFloat(6, p.price)
                            stmt.setBigDecimal(7, p.amount1)
                            stmt.setBigDecimal(8, p.amount2)
                            stmt.setInt(9, p.operation)
                            stmt.addBatch()
                        }
                        stmt.executeBatch().sum()
                    }
                }
                log.debug("Batch upserted latest prices: chunk size: {}, units: {}", cp.size, cp.map { it.time })
            }
        }
        log.debug("Total latest prices inserted/updated: {}", total)
        return total
    }

    fun batchInsertOrUpdateCombined(prices: List<Price>): Int {
        var total = 0
        database.useTransaction {
            prices.chunked(batchSize).forEach { chunk ->
                try {
                    total += database.useConnection { conn ->
                        val sql = """
                            WITH price_data AS (
                                SELECT
                                    unnest(?::bigint[]) AS asset_id,
                                    unnest(?::bigint[]) AS quote_asset_id,
                                    unnest(?::int[]) AS provider,
                                    unnest(?::bigint[]) AS time,
                                    unnest(?::boolean[]) AS outlier,
                                    unnest(?::bigint[]) AS tx_id,
                                    unnest(?::int[]) AS tx_swap_idx,
                                    unnest(?::float[]) AS price,
                                    unnest(?::numeric[]) AS amount1,
                                    unnest(?::numeric[]) AS amount2,
                                    unnest(?::int[]) AS operation
                            ),
                            insert_prices AS (
                                INSERT INTO price (
                                    asset_id, quote_asset_id, provider, time, outlier,
                                    tx_id, tx_swap_idx, price, amount1, amount2, operation
                                )
                                SELECT
                                    asset_id, quote_asset_id, provider, time, outlier,
                                    tx_id, tx_swap_idx, price, amount1, amount2, operation
                                FROM price_data
                                ON CONFLICT (time, tx_id, tx_swap_idx) DO UPDATE
                                SET
                                    asset_id = EXCLUDED.asset_id,
                                    quote_asset_id = EXCLUDED.quote_asset_id,
                                    provider = EXCLUDED.provider,
                                    price = EXCLUDED.price,
                                    amount1 = EXCLUDED.amount1,
                                    amount2 = EXCLUDED.amount2,
                                    operation = EXCLUDED.operation,
                                    outlier = EXCLUDED.outlier
                                RETURNING *
                            ),
                            latest_prices AS (
                                SELECT DISTINCT ON (asset_id, quote_asset_id, provider)
                                    asset_id, quote_asset_id, provider, time,
                                    tx_id, price, amount1, amount2, operation
                                FROM price_data
                                ORDER BY asset_id, quote_asset_id, provider, time DESC
                            )
                            INSERT INTO latest_price (
                                asset_id, quote_asset_id, provider, time,
                                tx_id, price, amount1, amount2, operation
                            )
                            SELECT
                                asset_id, quote_asset_id, provider, time,
                                tx_id, price, amount1, amount2, operation
                            FROM latest_prices
                            ON CONFLICT (asset_id, quote_asset_id, provider) DO UPDATE
                            SET
                                time = EXCLUDED.time,
                                tx_id = EXCLUDED.tx_id,
                                price = EXCLUDED.price,
                                amount1 = EXCLUDED.amount1,
                                amount2 = EXCLUDED.amount2,
                                operation = EXCLUDED.operation
                            RETURNING *
                        """.trimIndent()
                        conn.prepareStatement(sql).use { stmt ->
                            // Prepare arrays for each field
                            stmt.setArray(1, conn.createArrayOf("BIGINT", chunk.map { it.asset_id }.toTypedArray()))
                            stmt.setArray(
                                2,
                                conn.createArrayOf("BIGINT", chunk.map { it.quote_asset_id }.toTypedArray())
                            )
                            stmt.setArray(3, conn.createArrayOf("INTEGER", chunk.map { it.provider }.toTypedArray()))
                            stmt.setArray(4, conn.createArrayOf("BIGINT", chunk.map { it.time }.toTypedArray()))
                            stmt.setArray(5, conn.createArrayOf("BOOLEAN", chunk.map { it.outlier }.toTypedArray()))
                            stmt.setArray(6, conn.createArrayOf("BIGINT", chunk.map { it.tx_id }.toTypedArray()))
                            stmt.setArray(7, conn.createArrayOf("INTEGER", chunk.map { it.tx_swap_idx }.toTypedArray()))
                            stmt.setArray(8, conn.createArrayOf("FLOAT", chunk.map { it.price }.toTypedArray()))
                            stmt.setArray(9, conn.createArrayOf("NUMERIC", chunk.map { it.amount1 }.toTypedArray()))
                            stmt.setArray(10, conn.createArrayOf("NUMERIC", chunk.map { it.amount2 }.toTypedArray()))
                            stmt.setArray(11, conn.createArrayOf("INTEGER", chunk.map { it.operation }.toTypedArray()))

                            // Execute query and count affected rows
                            stmt.executeQuery().use { rs ->
                                var count = 0
                                while (rs.next()) {
                                    count++
                                }
                                count
                            }
                        }
                }
                log.debug("Batch upserted combined prices: chunk size: {}, units: {}", chunk.size, chunk.map { it.time })
            } catch (e: SQLException) {
                log.error("Failed to process combined batch of size ${chunk.size}", e)
                throw e // Rethrow to allow caller to handle
                }
            }
        }
        log.debug("Total combined prices inserted/updated: {}", total)
        return total
    }

    suspend fun asyncBatchInsertOrUpdate(prices: List<Price>): Int = withContext(Dispatchers.IO) {
        var total = 0
        database.useTransaction {
            prices.chunked(batchSize).map { cp ->
                async {
                    database.useConnection { conn ->
                        val sql = """
                        INSERT INTO price (
                            asset_id, quote_asset_id, provider, time, outlier, 
                            tx_id, tx_swap_idx, price, amount1, amount2, operation
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (time, tx_id, tx_swap_idx) DO UPDATE
                        SET asset_id = EXCLUDED.asset_id,
                            quote_asset_id = EXCLUDED.quote_asset_id,
                            provider = EXCLUDED.provider,
                            price = EXCLUDED.price,
                            amount1 = EXCLUDED.amount1,
                            amount2 = EXCLUDED.amount2,
                            operation = EXCLUDED.operation,
                            outlier = EXCLUDED.outlier
                    """.trimIndent()
                        conn.prepareStatement(sql).use { stmt ->
                            cp.forEach { p ->
                                stmt.setLong(1, p.asset_id)
                                stmt.setLong(2, p.quote_asset_id)
                                stmt.setInt(3, p.provider)
                                stmt.setLong(4, p.time)
                                if (p.outlier != null) {
                                    stmt.setBoolean(5, p.outlier!!)
                                } else {
                                    stmt.setNull(5, Types.BOOLEAN)
                                }
                                stmt.setLong(6, p.tx_id)
                                stmt.setInt(7, p.tx_swap_idx)
                                stmt.setFloat(8, p.price)
                                stmt.setBigDecimal(9, p.amount1)
                                stmt.setBigDecimal(10, p.amount2)
                                stmt.setInt(11, p.operation)
                                stmt.addBatch()
                            }
                            val batchCount = stmt.executeBatch().sum()
                            log.debug("Batch upserted prices: chunk size: {}, units: {}", cp.size, cp.map { it.time })
                            batchCount
                        }
                    }
                }
            }.awaitAll().sum().also { total = it }
        }
        log.debug("Total prices inserted/updated: {}", total)
        total
    }

    suspend fun asyncBatchInsertOrUpdateLatest(prices: List<Price>): Int = withContext(Dispatchers.IO) {
        val log = LoggerFactory.getLogger(PriceService::class.java)
        var total = 0
        database.useTransaction {
            prices.chunked(batchSize).map { cp ->
                async {
                    database.useConnection { conn ->
                        val sql = """
                        INSERT INTO latest_price (
                            asset_id, quote_asset_id, provider, time, 
                            tx_id, price, amount1, amount2, operation
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (asset_id, quote_asset_id, provider) DO UPDATE
                        SET time = EXCLUDED.time,
                            tx_id = EXCLUDED.tx_id,
                            price = EXCLUDED.price,                          
                            amount1 = EXCLUDED.amount1,
                            amount2 = EXCLUDED.amount2,
                            operation = EXCLUDED.operation
                    """.trimIndent()
                        conn.prepareStatement(sql).use { stmt ->
                            cp.forEach { p ->
                                stmt.setLong(1, p.asset_id)
                                stmt.setLong(2, p.quote_asset_id)
                                stmt.setInt(3, p.provider)
                                stmt.setLong(4, p.time)
                                stmt.setLong(5, p.tx_id)
                                stmt.setFloat(6, p.price)
                                stmt.setBigDecimal(7, p.amount1)
                                stmt.setBigDecimal(8, p.amount2)
                                stmt.setInt(9, p.operation)
                                stmt.addBatch()
                            }
                            val batchCount = stmt.executeBatch().sum()
                            log.debug("Batch upserted latest prices: chunk size: {}, units: {}", cp.size, cp.map { it.time })
                            batchCount
                        }
                    }
                }
            }.awaitAll().sum().also { total = it }
        }
        log.debug("Total latest prices inserted/updated: {}", total)
        total
    }

    fun getDistinctAssetPairs(fromTime: Long): List<Pair<Long, Long>> {
        return database
            .from(Prices)
            .selectDistinct(Prices.asset_id, Prices.quote_asset_id)
            .where { (Prices.time gte fromTime) and (Prices.outlier.isNull()) }
            .map { row ->
                Pair(
                    row[Prices.asset_id]!!,
                    row[Prices.quote_asset_id]!!
                )
            }
    }

    fun getPricesForPair(assetId: Long, quoteAssetId: Long, fromTime: Long): List<PriceRecord> {
        return database
            .from(Prices)
            .select(Prices.time, Prices.tx_id, Prices.tx_swap_idx, Prices.price)
            .where {
                (Prices.asset_id eq assetId) and
                        (Prices.quote_asset_id eq quoteAssetId) and
                        (Prices.time gte fromTime) and
                        (Prices.outlier.isNull())
            }
            .orderBy(Prices.time.asc())
            .map { row ->
                PriceRecord(
                    time = row[Prices.time]!!,
                    txId = row[Prices.tx_id]!!,
                    txSwapIdx = row[Prices.tx_swap_idx]!!,
                    price = row[Prices.price]!!.toDouble()
                )
            }
    }

    fun getRecentAveragePrice(assetId: Long, quoteAssetId: Long, fromTime: Long): Double? {
        return database
            .from(Prices)
            .select(avg(Prices.price))
            .where {
                (Prices.asset_id eq assetId) and
                        (Prices.quote_asset_id eq quoteAssetId) and
                        (Prices.time gte fromTime) and
                        (Prices.outlier.isNull())
            }
            .map { row -> row.getDouble(1).takeIf { !row.wasNull() } }
            .firstOrNull()
    }

    fun updateOutliers(outlierKeys: List<Triple<Long, Long, Int>>): Int {
        if (outlierKeys.isEmpty()) return 0
        var updatedRows = 0
        outlierKeys.chunked(1000).forEach { chunk ->
            chunk.forEach { triple ->
                updatedRows += database
                    .update(Prices) {
                        set(Prices.outlier, true)
                        where {
                            (Prices.time eq triple.first) and
                                    (Prices.tx_id eq triple.second) and
                                    (Prices.tx_swap_idx eq triple.third)
                        }
                    }
            }
        }
        return updatedRows
    }
}