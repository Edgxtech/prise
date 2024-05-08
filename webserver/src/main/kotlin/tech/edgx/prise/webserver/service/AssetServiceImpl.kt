package tech.edgx.prise.webserver.service

import com.bloxbean.cardano.client.util.HexUtil
import com.google.gson.Gson
import org.apache.commons.logging.LogFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tech.edgx.prise.webserver.repository.AssetRepository
import tech.edgx.prise.webserver.domain.AssetView
import tech.edgx.prise.webserver.model.prices.AssetPrice
import tech.edgx.prise.webserver.util.Helpers
import tech.edgx.prise.webserver.util.PricingProviderEnum

@Service("assetService")
@Transactional
class AssetServiceImpl : AssetService {

    @Autowired
    lateinit var assetRepository: AssetRepository

    @Throws(Exception::class)
    override fun getCNTPriceList(assetUnits: Set<String?>?): List<AssetPrice>? {
        val pricingProviders = PricingProviderEnum.entries.map { it.code }
        val assets = if (assetUnits.isNullOrEmpty()) {
            assetRepository.myFindCNT(pricingProviders)
        } else {
            val mutableAssetUnits = assetUnits.toMutableSet()
            val adaAssetUnit = assetUnits.filter { u: String? -> u == Helpers.ADA_ASSET_UNIT }.firstOrNull()
            if (adaAssetUnit != null) {
                mutableAssetUnits.remove(adaAssetUnit)
                log.debug("Requesting asset: " + Gson().toJson(mutableAssetUnits))
                assetRepository.myFindSpecifiedCNTAndAda(pricingProviders, mutableAssetUnits)
            } else {
                log.debug("Requesting asset: " + Gson().toJson(mutableAssetUnits))
                assetRepository.myFindSpecifiedCNT(pricingProviders, mutableAssetUnits)
            }
        }
        val prices = assets?.map { av: AssetView ->
                AssetPrice(
                    av.unit,
                    if (av.unit == Helpers.ADA_ASSET_UNIT) "ADA" else String(HexUtil.decodeHexString(av.unit.substring(56))),
                    av.price,
                    av.ada_price,
                    av.last_price_update,
                    av.pricing_provider
                )
            }
        return prices
    }

    override fun getDistinctSymbols(): Set<String>? {
        return assetRepository.myFindCNT(PricingProviderEnum.entries.map { it.code })?.map { a -> a.unit }?.toSet()
    }

    companion object {
        protected val log = LogFactory.getLog(
            AssetServiceImpl::class.java
        )
    }
}