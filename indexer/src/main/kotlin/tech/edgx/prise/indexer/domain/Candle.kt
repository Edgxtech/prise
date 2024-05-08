package tech.edgx.prise.indexer.domain

import org.ktorm.entity.Entity

interface Candle : Entity<Candle>, BasicCandle {
    companion object : Entity.Factory<Candle>()
    override val time: Long
    override val symbol: String
    override val open: Double?
    override val high: Double?
    override val low: Double?
    override val close: Double?
    override val volume: Double
}