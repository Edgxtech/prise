package tech.edgx.prise.indexer.service

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.TestInstance
import org.koin.core.parameter.parametersOf
import org.koin.test.inject
import tech.edgx.prise.indexer.Base
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.model.DexEnum
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AssetServiceIT: Base() {

    val assetService: AssetService by inject { parametersOf(config) }

    @Test
    fun isDatabaseUptoDate() {
        val a = Asset{
            unit = "test1"
            price = 0.1
            last_price_update = LocalDateTime.now()
        }
        assetService.insertOrUpdate(a)
        val upToDate = assetService.isDatabaseUptoDate(latestPricesTokenScrapeTime = 24L)
        println("up-to-date looking back 24 hours: $upToDate")
        assertTrue(upToDate)
        assetService.delete(a) // Cleanup
    }

    @Test
    fun crudTest() {
        assetService.truncateAllAssets()
        val testAsset = Asset.invoke {
            native_name = "testa"
            unit = "testunita"
            price = 0.1
            pricing_provider = DexEnum.WINGRIDERS.nativeName
        }
        assetService.insertOrUpdate(testAsset)
        val assets = assetService.getAllAssets()
        println("Comparing after insert: $testAsset, ${assets.first()}")
        assertTrue(testAsset == assets.first())
        /* update */
        testAsset.last_price_update = LocalDateTime.now()
        testAsset.price = 0.2
        assetService.insertOrUpdate(testAsset)
        val assetsAfterUpdate = assetService.getAllAssets()
        assertFalse(testAsset == assets.first())
        println("Comparing after update: $testAsset, ${assetsAfterUpdate.first()}")
        assertTrue(testAsset.unit == assetsAfterUpdate.first()?.unit)
        assertTrue(testAsset.native_name == assetsAfterUpdate.first()?.native_name)
        assertTrue(testAsset.price == assetsAfterUpdate.first()?.price)
        assertTrue(testAsset.pricing_provider == assetsAfterUpdate.first()?.pricing_provider)
        assertTrue(testAsset.last_price_update == assetsAfterUpdate.first()?.last_price_update)
        /* delete */
        assetService.truncateAllAssets()
        val assetsAfterDelete = assetService.getAllAssets()
        assertTrue(assetsAfterDelete.isEmpty())
    }
}