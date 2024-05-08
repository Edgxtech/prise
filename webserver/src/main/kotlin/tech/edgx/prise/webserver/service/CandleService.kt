package tech.edgx.prise.webserver.service

import tech.edgx.prise.webserver.domain.Candle
import tech.edgx.prise.webserver.domain.Close
import tech.edgx.prise.webserver.model.prices.PriceHistoryRequest

interface CandleService {
    fun getWeekly(priceHistoryRequest: PriceHistoryRequest): List<Candle?>?
    fun getWeeklyCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>?
    fun getDaily(priceHistoryRequest: PriceHistoryRequest): List<Candle?>?
    fun getDailyCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>?
    fun getHourly(priceHistoryRequest: PriceHistoryRequest): List<Candle?>?
    fun getHourlyCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>?
    fun getFifteen(priceHistoryRequest: PriceHistoryRequest): List<Candle?>?
    fun getFifteenCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>?
}
