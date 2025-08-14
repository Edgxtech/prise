package tech.edgx.prise.webserver.model.prices

import java.math.BigInteger

data class SwapDTO(
    val txHash: String,
    val slot: Long,
    val dex: Int,
    val asset1Unit: String,
    val asset2Unit: String,
    val amount1: BigInteger,
    val amount2: BigInteger,
    val operation: Int,
    val asset1Id: Long? = null,
    val asset2Id: Long? = null,
)