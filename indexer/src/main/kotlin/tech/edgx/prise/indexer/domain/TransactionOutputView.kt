package tech.edgx.prise.indexer.domain

data class TransactionOutputView(
    val txout_payload: ByteArray,
    val output_index: Int,
    val hash: String,
)