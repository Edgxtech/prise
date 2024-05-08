package tech.edgx.prise.indexer.domain

data class DexPriceHistoryView(
    val amount1: Long,
    val amount2: Long,
    val policy_id: String,
    val asset_name: String,
    val dex: Int,
    val slot: Long
)