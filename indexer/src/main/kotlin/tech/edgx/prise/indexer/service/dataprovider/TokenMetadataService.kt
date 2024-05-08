package tech.edgx.prise.indexer.service.dataprovider

import tech.edgx.prise.indexer.model.dataprovider.SubjectDecimalPair

interface TokenMetadataService {
    fun getDecimals(units: List<String>): List<SubjectDecimalPair>
}