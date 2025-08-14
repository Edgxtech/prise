package tech.edgx.prise.indexer.domain

import org.ktorm.entity.Entity
import org.ktorm.schema.*
import java.math.BigDecimal

object Prices : Table<Price>("price") {
    val asset_id = long("asset_id").bindTo { it.asset_id }
    val quote_asset_id = long("quote_asset_id").bindTo { it.quote_asset_id }
    val provider = int("provider").bindTo { it.provider }
    val time = long("time").primaryKey().bindTo { it.time }
    val tx_id = long("tx_id").primaryKey().bindTo { it.tx_id }
    val tx_swap_idx = int("tx_swap_idx").primaryKey().bindTo { it.tx_swap_idx }
    val price = float("price").bindTo { it.price }
    val amount1 = decimal("amount1").bindTo { it.amount1 }
    val amount2 = decimal("amount2").bindTo { it.amount2 }
    val operation = int("operation").bindTo { it.operation }
    val outlier = boolean("outlier").bindTo { it.outlier }
}

interface Price : Entity<Price> {
    companion object : Entity.Factory<Price>()
    var asset_id: Long
    var quote_asset_id: Long
    var provider: Int
    var time: Long
    var tx_id: Long
    var tx_swap_idx: Int
    var price: Float
    var amount1: BigDecimal
    var amount2: BigDecimal
    var operation: Int
    var outlier: Boolean?
}