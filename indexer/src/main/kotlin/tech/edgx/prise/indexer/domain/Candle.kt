package tech.edgx.prise.indexer.domain

import org.ktorm.entity.Entity

interface Candle : Entity<Candle>, BasicCandle {
    companion object : Entity.Factory<Candle>()
    override val time: Long
    override val asset_id: Long
    override val quote_asset_id: Long
    override val open: Float?
    override val high: Float?
    override val low: Float?
    override val close: Float?
    override val volume: Float
}