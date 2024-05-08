package tech.edgx.prise.indexer.service.dataprovider.module.carp.http.model

data class TransactionOutputRequest(
    val utxoPointers: List<Pointer>
)

data class PointerWithPayload(
    val txHash: String,
    val index: Int,
    val payload: String
)

data class Pointer(
    val txHash: String,
    val index: Int
)