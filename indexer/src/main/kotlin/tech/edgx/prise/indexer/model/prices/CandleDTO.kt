package tech.edgx.prise.indexer.model.prices

import tech.edgx.prise.indexer.domain.BasicCandle

data class CandleDTO(
    override val symbol: String,
    override val time: Long,
    override val open: Double?,
    override val high: Double?,
    override val low: Double?,
    override val close: Double?,
    override val volume: Double,
    ) : BasicCandle