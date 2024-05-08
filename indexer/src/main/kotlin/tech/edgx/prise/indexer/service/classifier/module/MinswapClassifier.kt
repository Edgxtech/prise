package tech.edgx.prise.indexer.service.classifier.module

import com.bloxbean.cardano.client.plutus.spec.PlutusData
import com.bloxbean.cardano.client.plutus.spec.serializers.PlutusDataJsonConverter
import com.bloxbean.cardano.client.util.JsonUtil
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.model.DexOperationEnum
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.classifier.common.ClassifierHelpers
import tech.edgx.prise.indexer.service.classifier.common.DexClassifierEnum
import tech.edgx.prise.indexer.util.Helpers
import java.math.BigInteger

object MinswapClassifier: DexClassifier {

    private val log = LoggerFactory.getLogger(javaClass)

    data class AmountOperationDTO(
        val amount1: BigInteger,
        val amount2: BigInteger,
        val swapDirection: Int
    )

    val DEX_CODE = 1
    val DEX_NAME = DexClassifierEnum.Minswap.name
    val POOL_SCRIPT_HASHES = listOf("e1317b152faac13426e6a83e06ff88a4d62cce3c1634ab0a5ec13309", "57c8e718c201fba10a9da1748d675b54281d3b1b983c5d1687fc7317")
    val ORDER_ADDRESSES_V1 = listOf("addr1wyx22z2s4kasd3w976pnjf9xdty88epjqfvgkmfnscpd0rg3z8y6v", "addr1wxn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uwc0h43gt")
    val ORDER_ADDRESSES_V2 = listOf("addr1zxn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uw6j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq6s3z70")
    val SWAP_IN_ADA: BigInteger = BigInteger.valueOf(4000000)
    val SWAP_OUT_ADA: BigInteger = BigInteger.valueOf(2000000)

    override fun getDexCode(): Int {
        return DEX_CODE
    }

    override fun getDexName(): String {
        return DEX_NAME
    }

    override fun getPoolScriptHash(): List<String> {
        return POOL_SCRIPT_HASHES
    }

    /* Pass this tx with all inputs, outputs, witnesses */
    override fun computeSwaps(txDTO: FullyQualifiedTxDTO) : List<Swap> {
        val swaps = mutableListOf<Swap>()
        log.debug("Computing swaps for tx: ${txDTO.txHash}")

        val outputDatumPair = txDTO.outputUtxos
            ?.firstOrNull { POOL_SCRIPT_HASHES.contains(Helpers.convertScriptAddressToPaymentCredential(it.address)) }
            ?.let { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)?: return emptyList()
                Pair(txOut, datum)
            }

        val lpDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(outputDatumPair?.second))
        log.debug("Extracted relevant output and datum: OUTPUT: ${outputDatumPair?.first} \n DATUM: ${lpDatumJsonNode}")

        val asset1Unit = when (lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.isEmpty) {
            true -> "lovelace"
            false -> lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                     lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                        ?: return listOf()
            null -> return listOf() // Metadata doesn't contain the asset unit, skip
        }
        val asset2Unit = lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                            lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                                ?: return listOf() // Metadata doesn't contain the asset unit, skip
        log.debug("Extracted asset 1: $asset1Unit, asset2 : $asset2Unit")


        /* Minswap V1 */
        // Operation info is contained in datum from inputs to the script
        val inputDatumPairsV1 = txDTO.inputUtxos
            .filter { ORDER_ADDRESSES_V1.contains(it.address) }
            .map { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)
                Pair(txOut, datum)
            }
        log.debug("Number of relevant inputs representing unique swaps, #: ${inputDatumPairsV1.size}")
        val versionSpecificSwapsV1 = computeVersionSpecificSwaps(txDTO, inputDatumPairsV1, asset1Unit, asset2Unit, DexEnum.MINSWAP)
        log.debug("V1 specific swaps: $versionSpecificSwapsV1")

        /* Minswap V2 */
        val inputDatumPairsV2 = txDTO.inputUtxos
            .filter { ORDER_ADDRESSES_V2.contains(it.address) }
            .map { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)
                Pair(txOut, datum)
            }
        val versionSpecificSwapsV2 = computeVersionSpecificSwaps(txDTO, inputDatumPairsV2, asset1Unit, asset2Unit, DexEnum.MINSWAPV2)
        log.debug("V2 specific swaps, #: ${versionSpecificSwapsV2.size}, V1 specific swaps, #: ${versionSpecificSwapsV1.size}")

        swaps.addAll(versionSpecificSwapsV1)
        swaps.addAll(versionSpecificSwapsV2)
        log.debug("Computed swaps: $swaps")
        return swaps
    }

    fun computeVersionSpecificSwaps(txDTO: FullyQualifiedTxDTO, inputDatumPairs: List<Pair<TransactionOutput, PlutusData?>>, asset1Unit: String, asset2Unit: String, dex: DexEnum): List<Swap> {
        val swaps = mutableListOf<Swap>()
        inputDatumPairs.forEach swaploop@{ (input, inputDatum) ->
            val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))

            val minswapOperation = lpInputDatumJsonNode?.get("fields")?.get(3)?.get("constructor")?.asInt()
            log.debug("Operation: $minswapOperation")
            if (minswapOperation != 0) {
                log.debug("Not a swap, skipping")
                return@swaploop
            }

            val swapToAssetUnit = lpInputDatumJsonNode.get("fields")?.get(3)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus(
                lpInputDatumJsonNode.get("fields")?.get(3)?.get("fields")?.get(0)?.get("fields")?.get(1)?.get("bytes")?.asText())
                ?: return@swaploop // couldnt get swap asset, skip
            log.debug("Extracted asset to swap to: $swapToAssetUnit")

            /* Find utxo corresponding to the swap input */
            val address = "01".plus(
                lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus(
                    lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()
                        ?: return@swaploop))
            log.debug("Address: $address")

            val output = txDTO.outputUtxos
                ?.firstOrNull {
                    log.debug("Comparing: ${it.address}, ${Helpers.convertScriptAddressToHex(it.address)} to $address")
                    Helpers.convertScriptAddressToHex(it.address) == address }?: return@swaploop

            val amountOperationDTO = when (swapToAssetUnit == asset2Unit) {
                true -> {
                    AmountOperationDTO(
                        input.amounts
                            .filter { it.unit == "lovelace" }
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?.minus(SWAP_IN_ADA)?: return@swaploop,
                        output.amounts
                            .filter { it.unit.replace(".","") == asset2Unit}
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?: return@swaploop,
                        DexOperationEnum.SELL.code
                    )
                }
                false -> {
                    AmountOperationDTO(
                        output.amounts
                            .filter { it.unit == "lovelace" }
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?.minus(SWAP_OUT_ADA)?: return@swaploop,
                        input.amounts
                            .filter { it.unit.replace(".","") == asset2Unit}
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?: return@swaploop,
                        DexOperationEnum.BUY.code
                    )
                }
            }
            log.debug("Amounts, 1: ${amountOperationDTO.amount1}, 2: ${amountOperationDTO.amount2}, operation: ${amountOperationDTO.swapDirection}")
            if (amountOperationDTO.amount1<=BigInteger.ZERO || amountOperationDTO.amount2<=BigInteger.ZERO) {
                log.debug("Had a zero or negative amount, skipping...")
                return@swaploop
            }

            val swap = Swap(
                txDTO.txHash,
                txDTO.blockSlot,
                dex.code,
                asset1Unit,
                asset2Unit,
                amountOperationDTO.amount1,
                amountOperationDTO.amount2,
                amountOperationDTO.swapDirection
            )
            log.debug("Computed swap: $swap")
            swaps.add(swap)
        }
        return swaps
    }
}