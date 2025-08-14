package tech.edgx.prise.indexer.processor

import com.bloxbean.cardano.client.util.HexUtil
import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.domain.Price
import tech.edgx.prise.indexer.event.PricesCalculatedEvent
import tech.edgx.prise.indexer.model.dataprovider.SubjectDecimalPair
import tech.edgx.prise.indexer.model.dex.SwapDTO
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.TxService
import tech.edgx.prise.indexer.service.PriceService
import tech.edgx.prise.indexer.service.dataprovider.TokenMetadataService
import tech.edgx.prise.indexer.service.dataprovider.common.TokenMetadataServiceEnum
import tech.edgx.prise.indexer.util.Helpers
import java.util.Collections

data class PriceConversionDTO(
    val swap: SwapDTO,
    val asset1: Asset?,
    val asset2: Asset?
)

class PriceProcessor(private val config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(this::class.java)
    private val assetService: AssetService by inject { parametersOf(config) }
    private val txService: TxService by inject { parametersOf(config) }
    private val priceService: PriceService by inject { parametersOf(config) }
    private val tokenMetadataService: TokenMetadataService by inject (named(TokenMetadataServiceEnum.tokenRegistry.name)) { parametersOf(config) }
    private val bufferedSwaps = Collections.synchronizedList(mutableListOf<PriceConversionDTO>())
    private val metadataFetchInterval = 20_000L
    private val scope = CoroutineScope(Dispatchers.IO + Job())

    init {
        scope.launch {
            while (true) {
                if (bufferedSwaps.isNotEmpty()) {
                    processBufferedSwaps()
                }
                delay(metadataFetchInterval)
            }
        }
    }

    fun processSwaps(swaps: List<SwapDTO>, blockSlot: Long): PricesCalculatedEvent {
        if (swaps.isEmpty()) return PricesCalculatedEvent(blockSlot, emptyList())

        val (prices, bufferedRemaining) = convertToPrices(swaps)
        if (prices.isNotEmpty()) {
            priceService.batchInsertOrUpdate(prices)
            log.debug("Persisted # prices: {}", prices.size)
        }
        bufferedSwaps.addAll(bufferedRemaining)
        log.debug("Buffered # swaps: {}", bufferedRemaining.size)
        return PricesCalculatedEvent(blockSlot, prices)
    }

    private fun convertToPrices(swaps: List<SwapDTO>, assets: Map<String, Asset>? = null): Pair<List<Price>, List<PriceConversionDTO>> {
        val prices = mutableListOf<Price>()
        val buffered = mutableListOf<PriceConversionDTO>()
        val units = swaps.flatMap { listOf(it.asset1Unit, it.asset2Unit) }.toSet()
        var assetsMap = assets ?: assetService.getAssetsByUnits(units)

        if (assets == null) {
            val missingUnits = units.filter { it !in assetsMap }.toSet()
            if (missingUnits.isNotEmpty()) {
                val newAssets = missingUnits.map { unit ->

                    val nativeName = if (unit == "lovelace") {
                        "ADA"
                    } else {
                        unit.takeIf { it.length > 56 }?.substring(56)?.let { hexName ->
                            try {
                                // Validate hex string
                                if (hexName.matches(Regex("^[0-9a-fA-F]+$"))) {
                                    String(HexUtil.decodeHexString(hexName)).replace("\u0000", "") // Remove null bytes
                                } else {
                                    log.warn("Invalid hex string for unit: $unit, hexName: $hexName")
                                    hexName // Fallback to hexName
                                }
                            } catch (e: Exception) {
                                log.error("Failed to decode hex for unit: $unit, hexName: $hexName", e)
                                hexName // Fallback to hexName on error
                            }
                        } ?: unit
                    }

                    Asset {
                        this.unit = unit
                        this.policy = unit.takeIf { it.length > 56 }?.substring(0, 56) ?: "lovelace"
                        this.native_name = nativeName
                        this.decimals = if (unit == "lovelace") 6 else null
                        this.metadata_fetched = if (unit == "lovelace") true else null
                    }
                }
                assetService.batchInsert(newAssets.groupBy { it.unit }.map { it.value.first() })
                assetsMap = assetService.getAssetsByUnits(units)
            }
        }

        val uniqueTxHashes = swaps.map { it.txHash }.toSet()
        val txHashToId = txService.batchInsertTxs(uniqueTxHashes.map { Helpers.hexToBinary(it) })
        val swapsByTxHash = swaps.groupBy { it.txHash }

        swapsByTxHash.forEach { (txHash, txSwaps) ->
            val txId = txHashToId[txHash] ?: throw IllegalStateException("tx_id not found for tx_hash: $txHash")
            txSwaps.forEachIndexed { index, swap ->
                val asset1 = assetsMap[swap.asset1Unit]
                val asset2 = assetsMap[swap.asset2Unit]
                if (asset1 == null || asset2 == null) {
                    buffered.add(PriceConversionDTO(swap, asset1, asset2))
                    return@forEachIndexed
                }
                if (asset1.metadata_fetched == true && asset2.metadata_fetched == true) {
                    prices.add(
                        Price {
                            time = swap.slot - Helpers.slotConversionOffset
                            tx_id = txId
                            tx_swap_idx = index
                            provider = swap.dex
                            asset_id = asset2.id
                            quote_asset_id = asset1.id
                            price = Helpers.calculatePriceInAsset1(
                                swap.amount1,
                                asset1.decimals ?: 0,
                                swap.amount2,
                                asset2.decimals ?: 0
                            )
                            amount1 = swap.amount1
                            amount2 = swap.amount2
                            operation = swap.operation
                        }
                    )
                } else {
                    buffered.add(PriceConversionDTO(swap, asset1, asset2))
                }
            }
        }
        return prices to buffered
    }

    private fun processBufferedSwaps() {
        synchronized(bufferedSwaps) {
            if (bufferedSwaps.isEmpty()) return
            val bufferedItems = bufferedSwaps.toList()
            bufferedSwaps.clear()

            val units = bufferedItems.flatMap { listOf(it.swap.asset1Unit, it.swap.asset2Unit) }.toSet()
            val currentAssets = assetService.getAssetsByUnits(units)
            val unitsToFetch = units.filter { it != "lovelace" && currentAssets[it]?.metadata_fetched != true }

            val subjectDecimalPairs = unitsToFetch.chunked(50).flatMap { chunk ->
                var attempts = 0
                while (attempts < 3) {
                    try {
                        return@flatMap tokenMetadataService.getDecimals(chunk)
                    } catch (e: Exception) {
                        attempts++
                        if (attempts == 3) throw e
                    }
                }
                emptyList<SubjectDecimalPair>()
            }
            val unitDecimalsMap = subjectDecimalPairs.associate { it.subject to it.decimals?.value }

            val assetsToUpdate = unitsToFetch.mapNotNull { unit ->
                val currentAsset = currentAssets[unit] ?: return@mapNotNull null
                currentAsset.apply {
                    decimals = unitDecimalsMap[unit]
                    metadata_fetched = true
                }
            }
            if (assetsToUpdate.isNotEmpty()) {
                assetService.batchUpdate(assetsToUpdate)
            }

            val updatedAssetsMap = assetService.getAssetsByUnits(units)
            val swaps = bufferedItems.map { it.swap }
            val (allPrices, remainingBuffered) = convertToPrices(swaps, updatedAssetsMap)
            bufferedSwaps.addAll(remainingBuffered)

            if (allPrices.isNotEmpty()) {
                priceService.batchInsertOrUpdate(allPrices)
            }
            log.info("Processed buffered swaps, updated # assets: {}, prices: {}, remaining buffered: {}", updatedAssetsMap.keys.size, allPrices.size, remainingBuffered.size)
        }
    }

    fun stop() {
        log.info("Stopping PriceProcessor")
        scope.cancel()
    }
}