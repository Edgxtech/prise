package tech.edgx.prise.indexer.service

import org.koin.core.component.KoinComponent
import org.koin.core.component.get
import org.koin.core.parameter.parametersOf
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.*
import tech.edgx.prise.indexer.repository.AssetRepository
import java.time.LocalDateTime

class AssetService(config: Config): KoinComponent {

    val assetRepository: AssetRepository = get { parametersOf(config.appDataSource) }

    fun save(asset: Asset) {
        assetRepository.save(asset)
    }

    fun insert(asset: Asset) {
        assetRepository.save(asset)
    }

    fun countAssets(): Int {
        return assetRepository.countAssets()
    }

    fun insertOrUpdate(asset: Asset) {
        assetRepository.insertOrUpdate(asset)
    }

    fun batchInsert(assets: List<Asset>): Int {
        return assetRepository.batchInsert(assets)
    }

    fun batchUpdate(assets: List<Asset>) {
        assetRepository.batchUpdate(assets)
    }

    fun batchUpdatePrices(assets: List<Asset>): Int {
        return assetRepository.batchUpdatePrices(assets)
    }

    fun batchInsertOrUpdate(assets: List<Asset>) {
        assetRepository.batchInsertOrUpdate(assets)
    }

    fun delete(asset: Asset) {
        assetRepository.delete(asset)
    }

    fun getAllAssets(): List<Asset?> {
        return assetRepository.getAllAssets()
    }

    fun getAllCNT(): List<Asset?> {
        return assetRepository.getAllCNT()
    }

    fun getAssetByUnit(unit: String): Asset? {
        return assetRepository.getByUnit(unit)
    }

    fun getAssetByUnitAndPricingProvider(unit: String, pricingProvider: String): Asset? {
        return assetRepository.getByUnitAndPricingProvider(unit, pricingProvider)
    }

    fun truncateAllAssets() {
        assetRepository.truncateAllAssets()
    }

    fun isDatabaseUptoDate(latestPricesTokenScrapeTime: Long): Boolean {
        return assetRepository.getLastAssetUpdate()
            ?.isAfter(LocalDateTime.now().minusHours(latestPricesTokenScrapeTime))?: false
    }
}