package tech.edgx.prise.indexer.service.price

import com.bloxbean.cardano.client.util.HexUtil
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.model.dataprovider.SubjectDecimalPair
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.dataprovider.TokenMetadataService
import tech.edgx.prise.indexer.util.Helpers
import java.time.LocalDateTime
import java.util.*

class LatestPriceService(config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    val assetService: AssetService by inject{ parametersOf(config) }
    val tokenMetadataService: TokenMetadataService by inject(named(config.tokenMetadataServiceModule))
    val updatedAssetBuffer = Collections.synchronizedList(mutableListOf<Asset>())
    var newAssetBuffer = Collections.synchronizedMap(mutableMapOf<String,Swap>())

    /* persist latest price from last swap. Note: asset2 refers to the non-ada asset, collect a bunch of these and batch insertOrUpdate on a timer */
    fun batchProcessLatestPrices(swap: Swap) {
        val asset2 = assetService.getAssetByUnitAndPricingProvider(swap.asset2Unit, Helpers.getDexName(swap.dex))
        when (asset2 == null) {
            true -> {
                /* Only retain the latest of each asset */
                log.debug("Found new asset: ${swap.asset2Unit}, buffering for bulk creation, new asset buffer size: ${newAssetBuffer.keys.size}")
                newAssetBuffer[swap.asset2Unit] = swap
            }
            else -> {
                /* otherwise batch update on a timer */
                asset2.ada_price = Helpers.calculatePriceInAsset1(
                    swap.amount1,
                    Helpers.ADA_ASSET_DECIMALS,
                    swap.amount2,
                    asset2.decimals)
                asset2.last_price_update = LocalDateTime.ofEpochSecond(swap.slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)
                log.debug("Queuing update of asset price: $asset2")
                updatedAssetBuffer.add(asset2)
            }
        }
    }

    fun updateAssetsNow(): Int {
        log.debug("# assets to batch update: ${updatedAssetBuffer.size}")
        val total = assetService.batchUpdatePrices(updatedAssetBuffer)
        /* Reset buffer */
        updatedAssetBuffer.clear()
        log.debug("# Asset updates in queue after clearing: ${updatedAssetBuffer.size}")
        return total
    }

    fun makeNewAssetsNow(): Int {
        synchronized(this) {
            val assetsToAdd = mutableListOf<Asset>()

            /* token registry can handle up to ~50 */
            val newAssetIds = newAssetBuffer.keys
            val subjectDecimalPairs: List<SubjectDecimalPair> = newAssetIds.toList()
                .chunked(50)
                .flatMap { tokenMetadataService.getDecimals(it) }
            log.debug("Token registry response: ${subjectDecimalPairs}, ${subjectDecimalPairs.size}")

            val unitDecimalsMap: Map<String, Int?> = subjectDecimalPairs.associate { it.subject to it.decimals?.value }

            /* Build new unique Assets */
            newAssetBuffer.values.forEach {
                val assetToAdd = tech.edgx.prise.indexer.domain.Asset {
                    policy = it.asset2Unit.substring(0, 56)
                    native_name = String(HexUtil.decodeHexString(it.asset2Unit.substring(56)))
                    unit = it.asset2Unit
                    ada_price = Helpers.calculatePriceInAsset1(
                        it.amount1,
                        Helpers.ADA_ASSET_DECIMALS,
                        it.amount2,
                        unitDecimalsMap[it.asset2Unit]
                    )
                    price = null
                    sidechain = null
                    decimals = unitDecimalsMap[it.asset2Unit]
                    incomplete_price_data = null
                    last_price_update =
                        LocalDateTime.ofEpochSecond(it.slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)
                    pricing_provider = Helpers.getDexName(it.dex)
                    logo_uri = null
                    preferred_name = null
                }
                log.debug("defined asset: $assetToAdd")
                assetsToAdd.add(assetToAdd)
            }
            log.debug("Adding # new assets: ${assetsToAdd.size}")
            val total = assetService.batchInsert(assetsToAdd)
            /* Reset buffer */
            newAssetBuffer.clear()
            return total
        }
    }
}