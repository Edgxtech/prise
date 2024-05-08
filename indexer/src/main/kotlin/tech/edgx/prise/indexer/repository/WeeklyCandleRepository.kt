package tech.edgx.prise.indexer.repository

import org.ktorm.database.Database
import org.ktorm.database.asIterable
import org.ktorm.dsl.*
import org.ktorm.entity.*
import org.ktorm.schema.BaseTable
import org.ktorm.support.mysql.bulkInsertOrUpdate
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Candle
import tech.edgx.prise.indexer.domain.LatestCandlesView
import tech.edgx.prise.indexer.domain.WeeklyCandles
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.util.Helpers
import java.time.LocalDateTime
import javax.sql.DataSource

class WeeklyCandleRepository(dataSource: DataSource) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val database = Database.connect(dataSource)
    var batch_size = 500

    val Database.candles get() = this.sequenceOf(WeeklyCandles)

    fun getByIdFromTo(symbol: String, from: Long, to: Long): List<Candle?>? {
        return database.candles
            .filter { it.symbol eq symbol }
            .filter { it.time gt from}
            .filter { it.time lt to }
            .toList()
    }

    fun getById(symbol: String): List<Candle> {
        return database.candles
            .filter { it.symbol eq symbol }
            .toList()
    }

    fun insert(c: CandleDTO) {
        database.insert(WeeklyCandles) {
                set(it.symbol, c.symbol)
                set(it.time, c.time)
                set(it.open, c.open)
                set(it.high, c.high)
                set(it.low, c.low)
                set(it.close, c.close)
                set(it.volume, c.volume)
        }
    }

    fun delete(candle: Candle) {
        database.candles.removeIf { it.symbol eq candle.symbol and (it.time eq candle.time) }
    }

    fun getLastCandle(from_asset_id: String): Candle? {
        return database.candles
            .filter { it.symbol eq from_asset_id }
            .sortedByDescending { it.time }
            .firstOrNull()
    }

    fun save(candle: Candle) {
        database.update(WeeklyCandles) {
            set(it.symbol, candle.symbol)
            set(it.time, candle.time)
            set(it.open, candle.open)
            set(it.high, candle.high)
            set(it.low, candle.low)
            set(it.close, candle.close)
            set(it.volume, candle.volume)
            where {
                it.symbol eq candle.symbol and ( it.time eq candle.time)
            }
        }
    }

    fun batchPersist(candles: List<CandleDTO>) {
        candles.chunked(batch_size).forEach { cc ->
            cc.forEach { c ->
                /* specific to ktorm-mysql */
                database.useTransaction {
                    database.bulkInsertOrUpdate(WeeklyCandles) {
                        item {
                            set(it.symbol, c.symbol)
                            set(it.time, c.time)
                            set(it.open, c.open)
                            set(it.high, c.high)
                            set(it.low, c.low)
                            set(it.close, c.close)
                            set(it.volume, c.volume)
                        }
                        onDuplicateKey {
                            set(it.open, c.open)
                            set(it.high, c.high)
                            set(it.low, c.low)
                            set(it.close, c.close)
                            set(it.volume, c.volume)
                        }
                    }
                }
            }
            log.debug ("Batch persisted weekly candles" )
        }
    }

    /* Used when batching/chunking is performed prior */
    fun persist(candles: List<CandleDTO>) {
        candles.forEach { c ->
            /* specific to ktorm-mysql */
            database.useTransaction {
                database.bulkInsertOrUpdate(WeeklyCandles) {
                    item {
                        set(it.symbol, c.symbol)
                        set(it.time, c.time)
                        set(it.open, c.open)
                        set(it.high, c.high)
                        set(it.low, c.low)
                        set(it.close, c.close)
                        set(it.volume, c.volume)
                    }
                    onDuplicateKey {
                        set(it.open, c.open)
                        set(it.high, c.high)
                        set(it.low, c.low)
                        set(it.close, c.close)
                        set(it.volume, c.volume)
                    }
                }
            }
        }
        log.debug("Persisted weekly candles")
    }

    fun Database.truncate(table: BaseTable<*>): Int {
        useConnection { conn ->
            conn.prepareStatement("truncate table ${table.tableName}").use { statement ->
                return statement.executeUpdate()
            }
        }
    }

//    fun getCandlesAtTime(fromTime: Long): List<LatestCandlesView> {
    fun getCandlesAtTime(fromTime: Long): List<CandleDTO> {
        val latestCandles = database.useConnection { conn ->
            val sql = "SELECT symbol, MAX(time) as time, open, high, low, close, volume FROM candle_weekly " +
                    "   WHERE time = ? GROUP BY symbol"
            conn.prepareStatement(sql).use { statement ->
                statement.setLong(1, fromTime)
                statement.executeQuery().asIterable().map {
                    CandleDTO(
                        it.getString("symbol"),
                        it.getLong("time"),
                        it.getDouble("open"),
                        it.getDouble("high"),
                        it.getDouble("low"),
                        it.getDouble("close"),
                        it.getDouble("volume"),
                    )
                }
            }
        }
        return latestCandles
    }

    /* Add continuation / zero volume candle, do it in one shot */
    fun addContinuationCandles(candleDtg: LocalDateTime, candleSymbolsMade: List<String>) {
        val candleDtgUnixTime = candleDtg.toEpochSecond(Helpers.zoneOffset)
        log.debug("Adding continuation candles for: $candleDtg, $candleDtgUnixTime")
        database.useConnection { conn ->
            /* cant use sql ARRAY type w/ mysql */
            val sql = String.format(
                "insert into candle_weekly (time, symbol, open, high, low, close, volume) " +
                        "select %d, c1.symbol, c1.close, c1.close, c1.close, c1.close, 0 " +
                        "    FROM candle_weekly c1 INNER JOIN " +
                        "    (SELECT symbol, time " +
                        "    FROM candle_weekly " +
                        "    WHERE time = %d " +
                        "    GROUP BY symbol) c2" +
                        "    ON c1.symbol=c2.symbol " +
                        "    AND c1.time=c2.time " +
                        "    WHERE c1.symbol not in (%s) " +
                        "on duplicate key update open=c1.close, high=c1.close, low=c1.close, close=c1.close",
                candleDtgUnixTime,
                candleDtgUnixTime - 7*24*60*60,
                candleSymbolsMade.joinToString(separator = "', '", prefix = "'", postfix = "'"))
            conn.prepareStatement(sql).use { statement ->
                val result = statement.executeUpdate()
                log.debug("executed batch update of continuation candles, result: $result")
            }
        }
    }

    /* Get continuation / zero volume candles */
    fun getContinuationCandleData(candleDtg: LocalDateTime, candleSymbolsMade: List<String>): List<CandleDTO> {
        val candleDtgUnixTime = candleDtg.toEpochSecond(Helpers.zoneOffset)
        log.debug("Getting last candles for: $candleDtg, $candleDtgUnixTime")
        val continuationCandleData = database.useConnection { conn ->
            /* cant use sql ARRAY type w/ mysql */
            val sql = String.format(
                "select %d as time, c1.symbol, c1.close " +
                        "    FROM candle_weekly c1 INNER JOIN " +
                        "    (SELECT symbol, time " +
                        "    FROM candle_weekly " +
                        "    WHERE time = %d " +
                        "    GROUP BY symbol) c2" +
                        "    ON c1.symbol=c2.symbol " +
                        "    AND c1.time=c2.time " +
                        "    WHERE c1.symbol not in (%s) ",
                candleDtgUnixTime,
                candleDtgUnixTime - 7*24*60*60,
                candleSymbolsMade.joinToString(separator = "', '", prefix = "'", postfix = "'")
            )
            conn.prepareStatement(sql).use { statement ->
                statement.executeQuery().asIterable().map {
                    CandleDTO(
                        it.getString("symbol"),
                        it.getLong("time"),
                        it.getDouble("close"),
                        it.getDouble("close"),
                        it.getDouble("close"),
                        it.getDouble("close"),
                        0.0
                    )
                }
            }
        }
        return continuationCandleData
    }
}