package tech.edgx.prise.webserver.model.prices

data class Candle(
    val time: Long,
    val symbol: String,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
    val volume: Double
)