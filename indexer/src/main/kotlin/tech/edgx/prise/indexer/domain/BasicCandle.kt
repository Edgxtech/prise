package tech.edgx.prise.indexer.domain

interface BasicCandle {
    val asset_id: Long
    val quote_asset_id: Long
    val time: Long
    val open: Float?
    val high: Float?
    val low: Float?
    val close: Float?
    val volume: Float?
}