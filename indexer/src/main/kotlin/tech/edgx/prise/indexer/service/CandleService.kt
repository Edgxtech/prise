package tech.edgx.prise.indexer.service

import org.koin.core.component.KoinComponent
import org.koin.core.component.get
import org.koin.core.parameter.parametersOf
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.Candle
import tech.edgx.prise.indexer.domain.LatestCandlesView
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.repository.*
import java.time.Duration
import java.time.LocalDateTime

class CandleService(config: Config): KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass::class.java)

    private val weeklyCandleRepository: WeeklyCandleRepository = get { parametersOf(config.appDataSource) }
    private val dailyCandleRepository: DailyCandleRepository = get { parametersOf(config.appDataSource) }
    private val hourlyCandleRepository: HourlyCandleRepository = get { parametersOf(config.appDataSource) }
    private val fifteenCandleRepository: FifteenCandleRepository = get { parametersOf(config.appDataSource) }
    private val baseCandleRepository: BaseCandleRepository = get { parametersOf(config.appDataSource) }

    fun getFifteenById(symbol: String): List<Candle> {
        return fifteenCandleRepository.getById(symbol)
    }

    fun getHourlyById(symbol: String): List<Candle> {
        return hourlyCandleRepository.getById(symbol)
    }

    fun getDailyById(symbol: String): List<Candle> {
        return dailyCandleRepository.getById(symbol)
    }

    fun getWeeklyById(symbol: String): List<Candle> {
        return weeklyCandleRepository.getById(symbol)
    }

    fun getWeeklyByIdFromTo(symbol: String, from: Long, to: Long): List<Candle?>? {
        return weeklyCandleRepository.getByIdFromTo(symbol, from, to)
    }

    fun getDailyByIdFromTo(symbol: String, from: Long, to: Long): List<Candle?>? {
        return dailyCandleRepository.getByIdFromTo(symbol, from, to)
    }

    fun getHourlyByIdFromTo(symbol: String, from: Long, to: Long): List<Candle> {
        return hourlyCandleRepository.getByIdFromTo(symbol, from, to)
    }

    fun getFifteenByIdFromTo(symbol: String, from: Long, to: Long): List<Candle> {
        return fifteenCandleRepository.getByIdFromTo(symbol, from, to)
    }

    fun getLastFifteenCandle(from_asset_id: String): Candle? {
        return fifteenCandleRepository.getLastCandle(from_asset_id)
    }

    fun getSyncPointTime(): Long? {
        return baseCandleRepository.getSyncPointTime()
    }

    fun getLastCandleForMultiple(): List<LatestCandlesView> {
        return baseCandleRepository.getLastCandleForMultiple()
    }

    /* Using this currently, handling batching at service layer */
    fun persistOrUpdate(candles: List<CandleDTO>, resoDuration: Duration) {
        when(resoDuration) {
            Duration.ofDays(7) -> weeklyCandleRepository.persist(candles)
            Duration.ofDays(1) -> dailyCandleRepository.persist(candles)
            Duration.ofHours(1) -> hourlyCandleRepository.persist(candles)
            Duration.ofMinutes(15) -> fifteenCandleRepository.persist(candles)
            else -> {
                log.warn("Reso duration temp skipped, not batch persisting: $resoDuration")
            }
        }
    }

    /* Option */
    fun batchPersistOrUpdate(candles: List<CandleDTO>, resoDuration: Duration) {
        when(resoDuration) {
            Duration.ofDays(7) -> weeklyCandleRepository.batchPersist(candles)
            Duration.ofDays(1) -> dailyCandleRepository.batchPersist(candles)
            Duration.ofHours(1) -> hourlyCandleRepository.batchPersist(candles)
            Duration.ofMinutes(15) -> fifteenCandleRepository.batchPersist(candles)
            else -> {
                log.warn("Reso duration temp skipped, not batch persisting: $resoDuration")
            }
        }
    }

    fun populateContinuationCandles(candleDtg: LocalDateTime, resoDuration: Duration, candleSymbolsMade: List<String>) {
        return when(resoDuration) {
            Duration.ofDays(7) -> weeklyCandleRepository.addContinuationCandles(candleDtg, candleSymbolsMade)
            Duration.ofDays(1) -> dailyCandleRepository.addContinuationCandles(candleDtg, candleSymbolsMade)
            Duration.ofHours(1) -> hourlyCandleRepository.addContinuationCandles(candleDtg, candleSymbolsMade)
            Duration.ofMinutes(15) -> fifteenCandleRepository.addContinuationCandles(candleDtg, candleSymbolsMade)
            else -> {
                log.warn("Reso duration temp skipped, not returning any candles for duration: $resoDuration")
            }
        }
    }

    fun getContinuationCandleData(candleDtg: LocalDateTime, resoDuration: Duration, candleSymbolsMade: List<String>): List<CandleDTO> {
        return when(resoDuration) {
            Duration.ofDays(7) -> weeklyCandleRepository.getContinuationCandleData(candleDtg, candleSymbolsMade)
            Duration.ofDays(1) -> dailyCandleRepository.getContinuationCandleData(candleDtg, candleSymbolsMade)
            Duration.ofHours(1) -> hourlyCandleRepository.getContinuationCandleData(candleDtg, candleSymbolsMade)
            Duration.ofMinutes(15) -> fifteenCandleRepository.getContinuationCandleData(candleDtg, candleSymbolsMade)
            else -> {
                log.warn("Reso duration temp skipped, not returning any candle continuation data for duration: $resoDuration")
                emptyList()
            }
        }
    }

    fun getCandlesAtTime(resoDuration: Duration, candleTime: Long): List<CandleDTO> { //LatestCandlesView
        return when(resoDuration) {
            Duration.ofDays(7) -> weeklyCandleRepository.getCandlesAtTime(candleTime)
            Duration.ofDays(1) -> dailyCandleRepository.getCandlesAtTime(candleTime)
            Duration.ofHours(1) -> hourlyCandleRepository.getCandlesAtTime(candleTime)
            Duration.ofMinutes(15) -> fifteenCandleRepository.getCandlesAtTime(candleTime)
            else -> {
                log.warn("Reso duration temp skipped, not returning any candles for duration: $resoDuration")
                listOf()
            }
        }
    }

    // Upgraded version of getCandlesAtTime
    fun getNCandlesBetweenTimes(resoDuration: Duration, from: Long, to: Long): List<LatestCandlesView> {
        return when(resoDuration) {
            Duration.ofDays(1) -> dailyCandleRepository.getNCandlesBetweenTimes(from, to)
            Duration.ofHours(1) -> hourlyCandleRepository.getNCandlesBetweenTimes(from, to)
            Duration.ofMinutes(15) -> fifteenCandleRepository.getNCandlesBetweenTimes(from, to)
            else -> {
                log.warn("Reso duration skipped, not returning any candles for duration: $resoDuration")
                listOf()
            }
        }
    }

    fun getNClosesForSymbolAtTime(resoDuration: Duration, numberRows: Int, symbol: String, candleTime: Long): List<Double> {
        return when(resoDuration) {
            Duration.ofMinutes(15) -> fifteenCandleRepository.getNClosesForSymbolBeforeTime(numberRows, symbol, candleTime)
            else -> {
                log.warn("Reso duration skipped, not returning any candles for duration: $resoDuration")
                listOf()
            }
        }
    }

    fun addIndexesIfRequired() {
        baseCandleRepository.addIndexesIfRequired()
    }
}