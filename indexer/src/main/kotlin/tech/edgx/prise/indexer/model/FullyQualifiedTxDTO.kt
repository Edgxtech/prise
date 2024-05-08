package tech.edgx.prise.indexer.model

import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.bloxbean.cardano.yaci.core.model.Witnesses

data class FullyQualifiedTxDTO(
    val txHash: String,
    val dexCode: Int,
    val dexCredential: String?,
    val blockSlot: Long,
    val inputUtxos: List<TransactionOutput>,
    val outputUtxos: List<TransactionOutput>?,
    val witnesses: Witnesses
)