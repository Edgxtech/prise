package tech.edgx.prise.webserver.domain

interface Candle {
    var time: Long?
    var symbol: String?
    var open: Double?
    var high: Double?
    var low: Double?
    var close: Double?
    var volume: Double?
}