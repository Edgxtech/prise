package tech.edgx.prise.indexer.model.prices

data class CandleDTO(
    val asset_id: Long,
    val quote_asset_id: Long,
    val time: Long,
    val open: Float,
    val high: Float,
    val low: Float,
    val close: Float?,
    val volume: Float
)