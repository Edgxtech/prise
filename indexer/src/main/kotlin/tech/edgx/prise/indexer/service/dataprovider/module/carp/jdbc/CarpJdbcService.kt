package tech.edgx.prise.indexer.service.dataprovider.module.carp.jdbc

import com.bloxbean.cardano.client.common.cbor.CborSerializationUtil
import com.bloxbean.cardano.client.util.HexUtil
import com.bloxbean.cardano.yaci.core.model.Amount
import com.bloxbean.cardano.yaci.core.model.TransactionInput
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.*
import tech.edgx.prise.indexer.repository.CarpRepository
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import java.util.concurrent.TimeUnit

class CarpJdbcService() : KoinComponent, ChainDatabaseService {
    private val log = LoggerFactory.getLogger(javaClass)

    val carpRepository: CarpRepository by inject()

    override fun getBlockNearestToSlot(slot: Long): BlockView? {
        var block: BlockView?
        var attempts = 0
        runBlocking {
            do {
                block = carpRepository.getBlockNearestToSlot(slot)
                if (block == null) {
                    log.info("carp not yet synced until slot: $slot")
                    if (attempts == 0 || attempts % 50 == 0) log.info("Carp not yet synced, couldnt retrieve block... waiting ... attempts: $attempts")
                    attempts++
                    delay(TimeUnit.SECONDS.toMillis(5))
                }
            } while (block == null)
        }
        return block
    }

    override fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput> {
        val txInHashes = txIns.map { t -> t.transactionId }.toTypedArray()
        val txInIndexes = txIns.map { t -> t.index }.toTypedArray()

        var attempts = 0
        var utxos: List<TransactionOutputView>
        /* prise chain sync can be ahead of carp, need to wait
        *  This is due to rollbacks taking a while on a large dataset; sometimes 5-10 mins */
        runBlocking {
            do {
                utxos = carpRepository.getTransactionOutput(txInHashes, txInIndexes)
                if (utxos.isEmpty()) {
                    log.debug("carp didnt yet have utxos: $txIns")
                    if (attempts==0 || attempts % 100 == 0) log.info("Carp not yet synced, couldnt retrieve utxos... waiting ... attempts: $attempts")
                    attempts++
                    delay(TimeUnit.SECONDS.toMillis(5))
                }
            } while (utxos.isEmpty())
        }

        log.trace("Requested input utxos: ${txInHashes.toList()}, ${txInIndexes.toList()}, Utxos, #: ${utxos.size}")
        val utxoRefMap = utxos.associateBy { it.hash+it.output_index }
        val sameOrderedUtxos = txIns.map { utxoRefMap[it.transactionId+it.index] }
        return sameOrderedUtxos
                    .map {it?.txout_payload?.let { txOutPayload ->
                        com.bloxbean.cardano.client.transaction.spec.TransactionOutput.deserialize(
                              CborSerializationUtil.deserialize(txOutPayload)) }?: return listOf() }
                    .map {
                        val amounts: MutableList<Amount> = it.value.multiAssets
                            .flatMap { ma -> ma.assets.map { Amount(ma.policyId+it.nameAsHex.substring(2), ma.policyId, it.nameAsHex.substring(2), null, it.value) } }
                            .toMutableList()
                        amounts.add(Amount("lovelace", "lovelace", "ada", null, it.value.coin))
                        TransactionOutput(
                            it.address,
                            amounts,
                            HexUtil.encodeHexString(it.datumHash),
                            it.inlineDatum?.serializeToHex(),//it.inlineDatum?.datumHash,
                            HexUtil.encodeHexString(it.scriptRef)) }
    }
}