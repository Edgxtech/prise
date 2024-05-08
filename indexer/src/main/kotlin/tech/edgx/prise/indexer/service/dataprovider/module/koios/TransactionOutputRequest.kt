package tech.edgx.prise.indexer.service.dataprovider.module.koios

data class TransactionOutputRequest(
    val _utxo_refs: List<String>,
    val _extended: Boolean
)