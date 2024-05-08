package tech.edgx.prise.webserver.service

import tech.edgx.prise.webserver.domain.Candle
import tech.edgx.prise.webserver.domain.Close
import tech.edgx.prise.webserver.model.prices.PriceHistoryRequest

interface PriceService {
    @Throws(Exception::class)
    fun getCandles(priceHistoryRequest: PriceHistoryRequest): List<Candle?>?

    @Throws(Exception::class)
    fun getCloses(priceHistoryRequest: PriceHistoryRequest): List<Close?>?
}
