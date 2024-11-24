package tech.edgx.prise.webserver.model.prices

data class Close(
    val time: Long,
    val symbol: String,
    val close: Double,
)