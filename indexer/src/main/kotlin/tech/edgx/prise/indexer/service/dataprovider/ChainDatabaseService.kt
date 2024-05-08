package tech.edgx.prise.indexer.service.dataprovider

import com.bloxbean.cardano.yaci.core.model.TransactionInput
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import tech.edgx.prise.indexer.domain.BlockView

interface ChainDatabaseService {
    fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput>
    fun getBlockNearestToSlot(slot: Long): BlockView?
}