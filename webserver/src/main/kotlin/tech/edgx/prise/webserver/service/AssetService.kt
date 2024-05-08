package tech.edgx.prise.webserver.service

import tech.edgx.prise.webserver.model.prices.AssetPrice

interface AssetService {
    fun getCNTPriceList(assetUnits: Set<String?>?): List<AssetPrice>?
    fun getDistinctSymbols(): Set<String>?
}
