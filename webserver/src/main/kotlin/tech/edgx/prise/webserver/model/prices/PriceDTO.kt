package tech.edgx.prise.webserver.model.prices

import java.math.BigDecimal

data class PriceDTO(
    val asset_id: Long,
    val quote_asset_id: Long,
    val provider: Int,
    val time: Long,
    val tx_id: Long,
    val tx_swap_idx: Int,
    val price: Double,
    val amount1: BigDecimal,
    val amount2: BigDecimal,
    val operation: Int
)