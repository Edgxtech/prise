package tech.edgx.prise.indexer.domain

data class BlockView(
    val hash: String,
    val epoch: Int,
    val height: Long,
    val slot: Long
)
