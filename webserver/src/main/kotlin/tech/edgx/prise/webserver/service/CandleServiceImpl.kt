package tech.edgx.prise.webserver.service

import org.apache.commons.logging.LogFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import tech.edgx.prise.webserver.repository.CandleDailyRepository
import tech.edgx.prise.webserver.repository.CandleFifteenRepository
import tech.edgx.prise.webserver.repository.CandleHourlyRepository
import tech.edgx.prise.webserver.repository.CandleWeeklyRepository
import tech.edgx.prise.webserver.domain.Candle
import tech.edgx.prise.webserver.domain.Close
import tech.edgx.prise.webserver.model.prices.PriceHistoryRequest
import java.util.*
import javax.transaction.Transactional

@Service("candleService")
@Transactional
class CandleServiceImpl : CandleService {

    companion object { protected val log = LogFactory.getLog(CandleServiceImpl::class.java) }

    @Autowired
    lateinit var candleWeeklyRepository: CandleWeeklyRepository

    @Autowired
    lateinit var candleDailyRepository: CandleDailyRepository

    @Autowired
    lateinit var candleHourlyRepository: CandleHourlyRepository

    @Autowired
    lateinit var candleFifteenRepository: CandleFifteenRepository

    override fun getWeekly(priceHistoryRequest: PriceHistoryRequest): List<Candle?>? {
        return candleWeeklyRepository.myFindByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }

    override fun getWeeklyCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>? {
        return candleWeeklyRepository.myFindClosesByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }

    override fun getDaily(priceHistoryRequest: PriceHistoryRequest): List<Candle?>? {
        return candleDailyRepository.myFindByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }

    override fun getDailyCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>? {
        return candleDailyRepository.myFindClosesByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }

    override fun getHourly(priceHistoryRequest: PriceHistoryRequest): List<Candle?>? {
        return candleHourlyRepository.myFindByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }

    override fun getHourlyCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>? {
        return candleHourlyRepository.myFindClosesByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }

    override fun getFifteen(priceHistoryRequest: PriceHistoryRequest): List<Candle?>? {
        return candleFifteenRepository.myFindByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }

    override fun getFifteenCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>? {
        return candleFifteenRepository.myFindClosesByIdFromTo(
            priceHistoryRequest.symbol,
            priceHistoryRequest.from,
            priceHistoryRequest.to
        )
    }
}