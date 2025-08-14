package tech.edgx.prise.indexer.domain

data class LatestCandlesView(
    val asset_id: Long,
    val quote_asset_id: Long,
    val time: Long,
    val open: Double?,
    val high: Double?,
    val low: Double?,
    val close: Double?,
    val volume: Double,
    val resolution: String
)