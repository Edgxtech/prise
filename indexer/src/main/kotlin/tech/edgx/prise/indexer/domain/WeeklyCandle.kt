package tech.edgx.prise.indexer.domain

import org.ktorm.schema.Table
import org.ktorm.schema.double
import org.ktorm.schema.long
import org.ktorm.schema.varchar

object WeeklyCandles : Table<Candle>("candle_weekly") {
    val symbol = varchar("symbol").primaryKey().bindTo { it.symbol }
    val time = long("time").primaryKey().bindTo { it.time }
    val open = double("open").bindTo { it.open }
    val high = double("high").bindTo { it.high }
    val low = double("low").bindTo { it.low }
    val close = double("close").bindTo { it.close }
    val volume = double("volume").bindTo { it.volume }
}