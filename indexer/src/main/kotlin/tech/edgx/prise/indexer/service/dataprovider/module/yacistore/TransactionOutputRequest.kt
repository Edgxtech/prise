package tech.edgx.prise.indexer.service.dataprovider.module.yacistore

data class TransactionOutputRequest(
    val tx_hash: String,
    val output_index: Int
)