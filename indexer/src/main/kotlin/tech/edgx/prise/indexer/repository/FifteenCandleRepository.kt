package tech.edgx.prise.indexer.repository

import org.ktorm.database.Database
import org.ktorm.database.asIterable
import org.ktorm.dsl.*
import org.ktorm.entity.*
import org.ktorm.support.mysql.bulkInsertOrUpdate
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Candle
import tech.edgx.prise.indexer.domain.FifteenCandles
import tech.edgx.prise.indexer.domain.LatestCandlesView
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.util.Helpers
import java.time.LocalDateTime
import javax.sql.DataSource

class FifteenCandleRepository(database: Database) {
    private val log = LoggerFactory.getLogger(javaClass::class.java)
    private val database = database
    var batch_size = 1000

    val Database.candles get() = this.sequenceOf(FifteenCandles)

    fun getByIdFromTo(symbol: String, from: Long, to: Long): List<Candle> {
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
        database.insert(FifteenCandles) {
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

    fun deleteAll() {
        database.candles.forEach { it.delete() }
    }

    fun getLastCandle(fromAssetId: String): Candle? {
        return database.candles
            .filter { it.symbol eq fromAssetId }
            .sortedByDescending { it.time }
            .firstOrNull()
    }

    fun save(candle: Candle) {
        database.update(FifteenCandles) {
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
                database.useTransaction {
                    database.bulkInsertOrUpdate(FifteenCandles) {
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
            log.debug ("Batch persisted fifteen candles" )
        }
    }

    /* Used when batching/chunking is performed prior */
    fun persist(candles: List<CandleDTO>) {
        log.debug("PERSISTING # Candles: ${candles.size}")
        candles.forEach { c ->
            /* specific to ktorm-mysql */
            database.useTransaction {
                database.bulkInsertOrUpdate(FifteenCandles) {
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
        log.debug("Persisted fifteen candles #: ${candles.size}")
    }

    fun getCandlesAtTime(fromTime: Long): List<CandleDTO> {
        val latestCandles = database.useConnection { conn ->
            val sql = "SELECT symbol, MAX(time) as time, open, high, low, close, volume FROM candle_fifteen " + //, '15m' as resolution
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
                        //it.getString("resolution")
                    )
                }
            }
        }
        return latestCandles
    }

    // NOTE: Cant use 'row numbers or distinct on' with mysql
    fun getNCandlesForSymbolsAtTime(numberRows: Int, symbols: Set<String>, candleTime: Long): List<LatestCandlesView> {
        val latestCandles = database.useConnection { conn ->
            /* cant use sql ARRAY type w/ mysql */
            val sql = String.format("SELECT symbol,time,open,high,low,close,volume, '15m' as resolution FROM " +
                    "( SELECT *, " +
                    "        @rn := IF(@prev = symbol, @rn + 1, 1) AS rn, " +
                    "        @prev := symbol  " +
                    "    FROM candle_fifteen  " +
                    "    JOIN (SELECT @prev := NULL, @rn := 0) AS vars " +
                    "    WHERE symbol in (%s) " +
                    "    AND time >= %d and time < %d " +
                    "    ORDER BY symbol, time ASC " +
                    ") as FILTERED_CANDLES " +
                    "WHERE rn <= %d",
                symbols.joinToString(separator = "', '", prefix = "'", postfix = "'"),
                candleTime,
                candleTime + 15*60*numberRows,
                numberRows)
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

    fun getNCandlesBetweenTimes(from: Long, to: Long): List<LatestCandlesView> {
        return database.useConnection { conn ->
            val sql = "SELECT symbol,time,open,high,low,close,volume, '15m' as resolution " +
                    "FROM candle_fifteen " +
                    "WHERE time >= ? and time < ?"
            conn.prepareStatement(sql).use { statement ->
                statement.setLong(1, from)
                statement.setLong(2, to)
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
    }

    fun getNClosesForSymbolBeforeTime(numberRows: Int, symbol: String, candleTime: Long): List<Double> {
        val closes = database.useConnection { conn ->
            val sql = "SELECT close from candle_fifteen WHERE symbol = ? " +
                    "   AND time < ? and time >= ?"
            conn.prepareStatement(sql).use { statement ->
                statement.setString(1, symbol)
                statement.setLong(2, candleTime)
                statement.setLong(3, candleTime - 15*60*numberRows)
                statement.executeQuery().asIterable().map {
                    it.getDouble("close")
                }
            }
        }
        return closes
    }

    /* Add continuation / zero volume candles */
    fun addContinuationCandles(candleDtg: LocalDateTime, candleSymbolsMade: List<String>) {
        val candleDtgUnixTime = candleDtg.toEpochSecond(Helpers.zoneOffset)
        log.debug("Adding continuation candles for: $candleDtg, $candleDtgUnixTime")
        database.useConnection { conn ->
            /* cant use sql ARRAY type w/ mysql */
            val sql = String.format(
                "insert into candle_fifteen (time, symbol, open, high, low, close, volume) " +
                        "select %d, c1.symbol, c1.close, c1.close, c1.close, c1.close, 0 " +
                        "    FROM candle_fifteen c1 INNER JOIN " +
                        "    (SELECT symbol, time " +
                        "    FROM candle_fifteen " +
                        "    WHERE time = %d " +
                        "    GROUP BY symbol) c2" +
                        "    ON c1.symbol=c2.symbol " +
                        "    AND c1.time=c2.time " +
                        "    WHERE c1.symbol not in (%s) " +
                        "on duplicate key update open=c1.close, high=c1.close, low=c1.close, close=c1.close",
                candleDtgUnixTime,
                candleDtgUnixTime - 15*60,
                candleSymbolsMade.joinToString(separator = "', '", prefix = "'", postfix = "'"))
            conn.prepareStatement(sql).use { statement ->
                val result = statement.executeUpdate()
                log.debug("executed batch update of continuation candles, result: $result")
            }
        }
    }

    fun getContinuationCandleData(candleDtg: LocalDateTime, candleSymbolsMade: List<String>): List<CandleDTO> {
        val candleDtgUnixTime = candleDtg.toEpochSecond(Helpers.zoneOffset)
        log.debug("Getting last candles for: $candleDtg, $candleDtgUnixTime")
        val continuationCandleData = database.useConnection { conn ->
            /* cant use sql ARRAY type w/ mysql */
            val sql = String.format(
                "select %d as time, c1.symbol, c1.close " +
                        "    FROM candle_fifteen c1 INNER JOIN " +
                        "    (SELECT symbol, time " +
                        "    FROM candle_fifteen " +
                        "    WHERE time = %d " +
                        "    GROUP BY symbol) c2" +
                        "    ON c1.symbol=c2.symbol " +
                        "    AND c1.time=c2.time " +
                        "    WHERE c1.symbol not in (%s) ",
                candleDtgUnixTime,
                candleDtgUnixTime - 15*60,
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