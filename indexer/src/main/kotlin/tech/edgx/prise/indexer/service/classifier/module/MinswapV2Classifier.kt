package tech.edgx.prise.indexer.service.classifier.module

import com.bloxbean.cardano.client.plutus.spec.serializers.PlutusDataJsonConverter
import com.bloxbean.cardano.client.util.JsonUtil
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

object MinswapV2Classifier: DexClassifier {

    private val log = LoggerFactory.getLogger(javaClass)

    data class AmountOperationDTO(
        val amount1: BigInteger,
        val amount2: BigInteger,
        val swapDirection: Int
    )

    val DEX_CODE = DexEnum.MINSWAPV2.code
    val DEX_NAME = DexClassifierEnum.MinswapV2.name
    val POOL_SCRIPT_HASHES = listOf("ea07b733d932129c378af627436e7cbc2ef0bf96e0036bb51b3bde6b")
    val ORDER_SCRIPT_HASHES = listOf("c3e28c36c3447315ba5a56f33da6a6ddc1770a876a8d9f0cb3a97c4c")

    val BATCHER_FEE: BigInteger = BigInteger.valueOf(1000000)
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
        log.debug("Computing swaps for tx: ${txDTO.txHash}")

        val outputDatumPair = txDTO.outputUtxos
            ?.firstOrNull { POOL_SCRIPT_HASHES.contains(Helpers.convertScriptAddressToPaymentCredential(it.address)) }
            ?.let { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)?: return emptyList()
                Pair(txOut, datum)
            }

        val lpDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(outputDatumPair?.second))
        log.debug("Extracted relevant output and datum: OUTPUT: ${outputDatumPair?.first} \n DATUM: ${lpDatumJsonNode}")

        val asset1Unit = when (lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("bytes")?.isEmpty) {
            true -> "lovelace"
            false -> lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                     lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                        ?: return listOf()
            null -> return listOf() // Metadata doesn't contain the asset unit, skip
        }
        val asset2Unit = lpDatumJsonNode.get("fields")?.get(2)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                            lpDatumJsonNode.get("fields")?.get(2)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                                ?: return listOf() // Metadata doesn't contain the asset unit, skip
        log.debug("Extracted asset 1: $asset1Unit, asset2 : $asset2Unit")

        val inputDatumPairs = txDTO.inputUtxos
            .filter { ORDER_SCRIPT_HASHES.contains(Helpers.convertScriptAddressToPaymentCredential(it.address)) }
            .map { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)
                Pair(txOut, datum)
            }

        val swaps = mutableListOf<Swap>()
        inputDatumPairs.forEach swaploop@{ (input, inputDatum) ->
            val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))
            log.debug("For swap, input datum: ${JsonUtil.getPrettyJson(lpInputDatumJsonNode)}")

            /* Currently only processing type==0; SWAP_EXACT_IN */
            val minswapOperation = lpInputDatumJsonNode?.get("fields")?.get(6)?.get("constructor")?.asInt()
            log.debug("Operation: $minswapOperation")
            if (minswapOperation != 0) {
                log.debug("Not a swap, skipping")
                return@swaploop
            }

            val minswapSwapDirection = lpInputDatumJsonNode.get("fields")?.get(6)?.get("fields")?.get(0)?.get("constructor")?.asInt()?: return@swaploop
            log.debug("Minswap Swap Direction: $minswapSwapDirection")

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

            val fee = lpInputDatumJsonNode.get("fields")?.get(7)?.get("int")?.asLong()?: return@swaploop
            log.debug("Fee: $fee")

            val amountOperationDTO = when (minswapSwapDirection == DexOperationEnum.BUY.code) {
                true -> {
                    log.debug("ADA input: ${input.amounts.filter { it.unit == "lovelace" }}")
                    val inputADA = input.amounts
                        .filter { it.unit == "lovelace" }
                        .map { it.quantity }
                        .reduceOrNull { a, b -> a.plus(b) }?: return@swaploop
                    log.debug("Input ADA: $inputADA")
                    val outputADA = output.amounts
                        .filter { it.unit == "lovelace" }
                        .map { it.quantity }
                        .reduceOrNull { a, b -> a.plus(b) }?: return@swaploop
                    log.debug("Input ADA: $inputADA, outputADA: $outputADA")
                    AmountOperationDTO(
                        inputADA?.minus(outputADA)?.minus(BigInteger.valueOf(fee))?: return@swaploop,
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
                getDexCode(),
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

    fun calculateZapSwapAmount(amountIn: BigInteger, reserveIn: BigInteger, reserveOut: BigInteger, totalLpToken: BigInteger) {
        //swapAmountIn = (sqrt(1997n ** 2n * reserveIn ** 2n + 4n * 997n * 1000n * amountIn * reserveIn) - 1997n * reserveIn) / (2n * 997n);
    }
}