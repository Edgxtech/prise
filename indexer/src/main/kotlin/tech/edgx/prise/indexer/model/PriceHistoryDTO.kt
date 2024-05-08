package tech.edgx.prise.indexer.model

import java.time.LocalDateTime

data class PriceHistoryDTO(
    val ldt: LocalDateTime = LocalDateTime.now(),
    val price: Double?,
    val volume: Double = 0.0
)