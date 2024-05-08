package tech.edgx.prise.indexer.repository

import org.ktorm.database.Database
import org.ktorm.database.asIterable
import org.ktorm.schema.BaseTable
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.*
import javax.sql.DataSource

class BaseCandleRepository(dataSource: DataSource) {
    private val log = LoggerFactory.getLogger(javaClass::class.java)
    private val database = Database.connect(dataSource)

    fun Database.truncate(table: BaseTable<*>): Int {
        useConnection { conn ->
            conn.prepareStatement("truncate table ${table.tableName}").use { statement ->
                return statement.executeUpdate()
            }
        }
    }

    fun truncateAllCandles() {
        database.truncate(WeeklyCandles)
        database.truncate(DailyCandles)
        database.truncate(HourlyCandles)
        database.truncate(FifteenCandles)
    }

    // NOTE: Cant use 'distinct on' with mysql
    fun getLastCandleForMultiple(): List<LatestCandlesView> {
        val latestCandles = database.useConnection { conn ->
            val sql = "SELECT c1.symbol, c1.time, c1.open, c1.high, c1.low, c1.close, c1.volume, '1W' as resolution " +
                    "            FROM candle_weekly c1 INNER JOIN " +
                    "            (SELECT symbol, MAX(time) as m_time " +
                    "            FROM candle_weekly GROUP BY symbol) c2 " +
                    "            ON c1.symbol=c2.symbol " +
                    "            AND c1.time=c2.m_time " +
                    "   UNION SELECT c1.symbol, c1.time, c1.open, c1.high, c1.low, c1.close, c1.volume, '1D' as resolution " +
                    "            FROM candle_daily c1 INNER JOIN " +
                    "            (SELECT symbol, MAX(time) as m_time " +
                    "            FROM candle_daily GROUP BY symbol) c2 " +
                    "            ON c1.symbol=c2.symbol " +
                    "            AND c1.time=c2.m_time" +
                    "   UNION SELECT c1.symbol, c1.time, c1.open, c1.high, c1.low, c1.close, c1.volume, '1h' as resolution " +
                    "            FROM candle_hourly c1 INNER JOIN " +
                    "            (SELECT symbol, MAX(time) as m_time " +
                    "            FROM candle_hourly GROUP BY symbol) c2 " +
                    "            ON c1.symbol=c2.symbol " +
                    "            AND c1.time=c2.m_time" +
                    "   UNION SELECT c1.symbol, c1.time, c1.open, c1.high, c1.low, c1.close, c1.volume, '15m' as resolution " +
                    "            FROM candle_fifteen c1 INNER JOIN " +
                    "            (SELECT symbol, MAX(time) as m_time " +
                    "            FROM candle_fifteen GROUP BY symbol) c2 " +
                    "            ON c1.symbol=c2.symbol " +
                    "            AND c1.time=c2.m_time";
            conn.prepareStatement(sql).use { statement ->
                statement.executeQuery().asIterable().map {
                    LatestCandlesView(
                        it.getString("symbol"),
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
            val sql = "with times as (SELECT max(time) as time from candle_fifteen group by symbol having time is not null) " +
                      "select min(time) as sync_point_time from times"
            conn.prepareStatement(sql).use { statement ->
                statement.executeQuery().asIterable().map {
                    it.getLong("sync_point_time")
                }
            }
        }.firstOrNull()
        return when (syncPointTime!=0L) {
            true -> syncPointTime
            else -> null
        }
    }

    fun addIndexesIfRequired() {
        val tableNames = listOf("candle_fifteen", "candle_hourly", "candle_daily", "candle_weekly")
        val tablesWithMissingIndex = tableNames.map { tableName ->
            val timeIdx = database.useConnection { conn ->
                val sql = "SHOW INDEX FROM $tableName"
                conn.prepareStatement(sql).use { statement ->
                    statement.executeQuery().asIterable().map {
                        it.getString("Key_name")
                    }
                }
            }.firstOrNull { it == "time" }
            if (timeIdx==null) {
                tableName
            } else null
        }.filterNotNull()
        if (tablesWithMissingIndex.isNotEmpty()) log.info("Adding table indexes for: $tablesWithMissingIndex")
        tablesWithMissingIndex.forEach {tableName ->
            database.useConnection { conn ->
                val sql = "ALTER TABLE $tableName ADD INDEX (time)"
                conn.prepareStatement(sql).use { statement ->
                    statement.executeUpdate()
                }
            }
            log.info("Successfully added table indexes for: $tableName")
        }
    }
}