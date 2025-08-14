package tech.edgx.prise.indexer.model.dex

import java.math.BigDecimal

data class SwapDTO(
    val txHash: String,
    val slot: Long,
    val dex: Int,
    val asset1Unit: String,
    val asset2Unit: String,
    val amount1: BigDecimal,
    val amount2: BigDecimal,
    val operation: Int
)