package tech.edgx.prise.webserver.domain

interface Candle {
    var time: Long?
    var asset_id: Long?
    var quote_asset_id: Long?
    var open: Double?
    var high: Double?
    var low: Double?
    var close: Double?
    var volume: Double?
}

