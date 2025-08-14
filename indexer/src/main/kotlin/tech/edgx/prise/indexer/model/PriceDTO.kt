package tech.edgx.prise.indexer.model

import java.time.LocalDateTime

data class PriceDTO(
    val ldt: LocalDateTime = LocalDateTime.now(),
    val price: Float?,
    val volume: Float = 0.0F
)