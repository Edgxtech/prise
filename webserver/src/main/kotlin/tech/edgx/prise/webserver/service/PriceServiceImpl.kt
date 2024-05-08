package tech.edgx.prise.webserver.service

import com.google.gson.Gson
import javax.annotation.Resource
import org.apache.commons.logging.LogFactory
import org.springframework.stereotype.Service
import tech.edgx.prise.webserver.domain.Candle
import tech.edgx.prise.webserver.domain.Close
import tech.edgx.prise.webserver.model.prices.PriceHistoryRequest
import tech.edgx.prise.webserver.util.Helpers
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.*

@Service("priceService")
class PriceServiceImpl : PriceService {
    @Resource(name = "candleService")
    lateinit var candleService: CandleService
    val zoneOffset: ZoneOffset = ZoneId.systemDefault().rules.getOffset(LocalDateTime.now())

    // Sundaeswap first launched ~Jan 20 2022, Pick 01 Jan 2022
    var dexLaunchDate = LocalDateTime.of(2022, 1, 1, 0, 0, 0)

    fun determineDateFromTo(priceHistoryRequest: PriceHistoryRequest): Array<Long?> {
        var to: Long? = Date().time / 1000
        val from = if (priceHistoryRequest.from != null) {
            priceHistoryRequest.to
        } else {
            // start at the first known dex launch/trading date
            dexLaunchDate.toEpochSecond(zoneOffset)
        }
        if (priceHistoryRequest.to != null) {
            to = priceHistoryRequest.to
        }
        log.debug("Getting price data from: " + Gson().toJson(from) + ", to: " + Gson().toJson(to))
        return arrayOf(from, to)
    }

    @Throws(Exception::class)
    override fun getCandles(priceHistoryRequest: PriceHistoryRequest): List<Candle?>? {
        val datesFromTo = determineDateFromTo(priceHistoryRequest)
        priceHistoryRequest.from=(datesFromTo[0])
        priceHistoryRequest.to=(datesFromTo[1])
        val candles: List<Candle?> = ArrayList()
        log.debug("Getting pre-indexed candles: " + Gson().toJson(priceHistoryRequest))
        if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_1W) {
            return candleService.getWeekly(priceHistoryRequest)
        } else if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_1D) {
            return candleService.getDaily(priceHistoryRequest)
        } else if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_1H) {
            return candleService.getHourly(priceHistoryRequest)
        } else if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_15M) {
            return candleService.getFifteen(priceHistoryRequest)
        }
        return candles
    }

    @Throws(Exception::class)
    override fun getCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>? {
        val datesFromTo = determineDateFromTo(priceHistoryRequest)
        priceHistoryRequest.from=(datesFromTo[0])
        priceHistoryRequest.to=(datesFromTo[1])
        val closes: List<Close?> = ArrayList()
        log.debug("Getting pre-indexed closes: " + Gson().toJson(priceHistoryRequest))
        if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_1W) {
            return candleService.getWeeklyCloses(priceHistoryRequest)
        } else if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_1D) {
            return candleService.getDailyCloses(priceHistoryRequest)
        } else if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_1H) {
            return candleService.getHourlyCloses(priceHistoryRequest)
        } else if (priceHistoryRequest.resolution == Helpers.RESO_DEFN_15M) {
            return candleService.getFifteenCloses(priceHistoryRequest)
        }
        return closes
    }

    companion object {
        protected val log = LogFactory.getLog(
            PriceServiceImpl::class.java
        )
        var durationMinutesMap: MutableMap<Duration, Int> = HashMap()

        init {
            durationMinutesMap[Duration.ofDays(7)] = 10080
            durationMinutesMap[Duration.ofDays(1)] = 1440
            durationMinutesMap[Duration.ofHours(1)] = 60
            durationMinutesMap[Duration.ofMinutes(15)] = 15
        }
    }
}