package tech.edgx.prise.indexer.processor

import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.model.TransactionBody
import com.bloxbean.cardano.yaci.core.model.Witnesses
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.event.SwapsComputedEvent
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.util.Helpers

class SwapProcessor(val config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(this::class.java)
    private val chainDatabaseService: ChainDatabaseService by inject (named(config.chainDatabaseServiceModule)) { parametersOf(config) }
    private val dexClassifiers: List<DexClassifier> by inject(named("dexClassifiers"))
    private val dexClassifierMap: Map<String, DexClassifier> = dexClassifiers
        .flatMap { it.getPoolScriptHash().map { hash -> hash to it } }
        .filter { config.dexClassifiers?.contains(it.second.getDexName()) ?: false }
        .toMap()

    fun processBlock(block: Block): SwapsComputedEvent {
        val blockSlot = block.header.headerBody.slot
        val qualifiedTxs = qualifyTransactions(blockSlot, block.transactionBodies, block.transactionWitness)
        val swaps = qualifiedTxs.flatMap { tx ->
            dexClassifierMap[tx.dexCredential]?.computeSwaps(tx).orEmpty()
        }
        log.debug("Found # swaps: {}", swaps.size)
        return SwapsComputedEvent(blockSlot, swaps)
    }

    fun qualifyTransactions(blockSlot: Long, transactionBodies: List<TransactionBody>, transactionWitnesses: List<Witnesses>): List<FullyQualifiedTxDTO> {
        log.debug("Qualifying transactions: block slot: {}", blockSlot)
        val filteredTxAndWitnesses = transactionBodies.zip(transactionWitnesses)
            .filter { it.first.outputs.any { o -> dexClassifierMap.keys.contains(Helpers.convertScriptAddressToPaymentCredential(o.address)) } }
        return filteredTxAndWitnesses.map { (txBody, witnesses) ->
            val dexCredentialMatched = txBody.outputs
                .map { Helpers.convertScriptAddressToPaymentCredential(it.address) }
                .first { dexClassifierMap.keys.contains(it) }
            FullyQualifiedTxDTO(
                txBody.txHash,
                Helpers.resolveDexNumFromCredential(dexCredentialMatched),
                dexCredentialMatched,
                blockSlot,
                chainDatabaseService.getInputUtxos(txBody.inputs),
                txBody.outputs,
                witnesses
            )
        }.filter { it.inputUtxos.isNotEmpty() }
    }
}