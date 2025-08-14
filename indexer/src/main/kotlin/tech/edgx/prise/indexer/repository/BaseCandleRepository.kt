package tech.edgx.prise.indexer.repository

import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import org.ktorm.database.Database
import org.ktorm.database.asIterable
import org.ktorm.schema.BaseTable
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.*

class BaseCandleRepository : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    private val database: Database by inject(named("appDatabase"))

    fun Database.truncate(table: BaseTable<*>): Int {
        useConnection { conn ->
            conn.prepareStatement("truncate table ${table.tableName}").use { statement ->
                return statement.executeUpdate()
            }
        }
    }

    fun Database.deleteAll(table: BaseTable<*>): Int {
        useConnection { conn ->
            conn.prepareStatement("delete from ${table.tableName}").use { statement ->
                return statement.executeUpdate()
            }
        }
    }

    fun getLastCandleForMultiple(): List<LatestCandlesView> {
        val latestCandles = database.useConnection { conn ->
            val sql = """
                SELECT c1.asset_id, c1.quote_asset_id, c1.time, c1.open, c1.high, c1.low, c1.close, c1.volume, '15m' AS resolution
                FROM candle_fifteen c1
                INNER JOIN (
                    SELECT asset_id, quote_asset_id, MAX(time) AS m_time
                    FROM candle_fifteen
                    GROUP BY asset_id, quote_asset_id
                ) c2 ON c1.asset_id = c2.asset_id AND c1.quote_asset_id = c2.quote_asset_id AND c1.time = c2.m_time
            """.trimIndent()
            conn.prepareStatement(sql).use { statement ->
                statement.executeQuery().asIterable().map {
                    LatestCandlesView(
                        it.getLong("asset_id"),
                        it.getLong("quote_asset_id"),
                        it.getLong("time"),
                        it.getDouble("open"),
                        it.getDouble("high"),
                        it.getDouble("low"),
                        it.getDouble("close"),
                        it.getDouble("volume"),
                        it.getString("resolution")
                    )
                }
            }
        }
        return latestCandles
    }

    fun getSyncPointTime(): Long? {
        val syncPointTime = database.useConnection { conn ->
            val sql = """
                WITH times AS (
                    SELECT MAX(time) AS time 
                    FROM candle_fifteen 
                    GROUP BY asset_id, quote_asset_id 
                    HAVING time IS NOT NULL
                )
                SELECT MIN(time) AS sync_point_time 
                FROM times
            """.trimIndent()
            conn.prepareStatement(sql).use { statement ->
                statement.executeQuery().asIterable().map {
                    it.getLong("sync_point_time")
                }
            }
        }.firstOrNull()
        return when (syncPointTime != 0L) {
            true -> syncPointTime
            else -> null
        }
    }
}
