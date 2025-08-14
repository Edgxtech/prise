package tech.edgx.prise.indexer.service.classifier

import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.SwapDTO

interface DexClassifier {
    fun getDexCode(): Int
    fun getDexName(): String
    fun getPoolScriptHash(): List<String>
    fun computeSwaps(txDTO: FullyQualifiedTxDTO): List<SwapDTO>
}