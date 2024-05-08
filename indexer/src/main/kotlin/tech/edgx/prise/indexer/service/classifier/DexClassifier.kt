package tech.edgx.prise.indexer.service.classifier

import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.Swap

interface DexClassifier {
    fun getDexCode(): Int
    fun getDexName(): String
    fun getPoolScriptHash(): List<String>
    fun computeSwaps(txDTO: FullyQualifiedTxDTO): List<Swap>
}