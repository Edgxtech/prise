package tech.edgx.prise.indexer.domain

interface BasicCandle {
    val symbol: String
    val time: Long
    val open: Double?
    val high: Double?
    val low: Double?
    val close: Double?
    val volume: Double?
}