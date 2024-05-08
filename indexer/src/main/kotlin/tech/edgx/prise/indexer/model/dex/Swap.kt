package tech.edgx.prise.indexer.model.dex

import java.math.BigInteger

data class Swap(
    val txHash: String,
    val slot: Long,
    val dex: Int,
    val asset1Unit: String,
    val asset2Unit: String,
    val amount1: BigInteger,
    val amount2: BigInteger,
    val operation: Int
)