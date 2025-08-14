package tech.edgx.prise.indexer.service.classifier.module

import com.bloxbean.cardano.client.plutus.spec.serializers.PlutusDataJsonConverter
import com.bloxbean.cardano.client.util.JsonUtil
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.fasterxml.jackson.databind.JsonNode
import org.apache.commons.math.util.MathUtils
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.model.DexOperationEnum
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.SwapDTO
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.classifier.common.ClassifierHelpers
import tech.edgx.prise.indexer.service.classifier.common.DexClassifierEnum
import tech.edgx.prise.indexer.util.Helpers
import java.math.BigInteger
import java.math.RoundingMode
import com.google.common.math.BigIntegerMath.sqrt

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
    val DEFAULT_TRADING_FEE_DENOMINATOR = BigInteger.valueOf(10000)

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
    override fun computeSwaps(txDTO: FullyQualifiedTxDTO) : List<SwapDTO> {
        log.debug("Computing swaps for tx: ${txDTO.txHash}")

        val poolDatumPair = txDTO.inputUtxos
            .firstOrNull { POOL_SCRIPT_HASHES.contains(Helpers.convertScriptAddressToPaymentCredential(it.address)) }
            ?.let { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)?: return emptyList()
                Pair(txOut, datum)
            }

        val poolDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(poolDatumPair?.second))
        log.debug("Extracted relevant output and datum: OUTPUT: ${poolDatumPair?.first} \n DATUM: ${poolDatumJsonNode}")

        val asset1Unit = when (poolDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("bytes")?.isEmpty) {
            true -> "lovelace"
            false -> poolDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                poolDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                        ?: return listOf()
            null -> return listOf() // Metadata doesn't contain the asset unit, skip
        }
        val asset2Unit = poolDatumJsonNode.get("fields")?.get(2)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                            poolDatumJsonNode.get("fields")?.get(2)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                                ?: return listOf() // Metadata doesn't contain the asset unit, skip
        log.debug("Extracted asset 1: $asset1Unit, asset2 : $asset2Unit")

        val inputDatumPairs = txDTO.inputUtxos
            .filter { ORDER_SCRIPT_HASHES.contains(Helpers.convertScriptAddressToPaymentCredential(it.address)) }
            .map { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)
                Pair(txOut, datum)
            }

        val swaps = inputDatumPairs.mapNotNull { (input, inputDatum) ->
            val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))
            log.debug("For swap, input datum: ${JsonUtil.getPrettyJson(lpInputDatumJsonNode)}")

            /* Currently only processing type 0: SWAP_EXACT_IN, 4: DEPOSIT */
            val minswapOperation = lpInputDatumJsonNode?.get("fields")?.get(6)?.get("constructor")?.asInt()
            log.debug("Operation: $minswapOperation")
            val swap = when (minswapOperation) {
                0 -> computeSwapExactIn(lpInputDatumJsonNode, txDTO, input, asset1Unit, asset2Unit)
                4 -> computeZapIn(poolDatumJsonNode, lpInputDatumJsonNode, txDTO, asset1Unit, asset2Unit)
                else -> {
                    log.debug("Not a swap, skipping")
                    null
                }
            }
            swap
        }
        return swaps
    }

    fun computeSwapExactIn(lpInputDatumJsonNode: JsonNode, txDTO: FullyQualifiedTxDTO, input: TransactionOutput, asset1Unit: String, asset2Unit: String): SwapDTO? {
        val minswapSwapDirection = lpInputDatumJsonNode.get("fields")?.get(6)?.get("fields")?.get(0)?.get("constructor")?.asInt()?: return null
        log.debug("Minswap Swap Direction: $minswapSwapDirection")

        /* Find utxo corresponding to the swap input */
        val address = "01".plus(
            lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus(
                lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()
                    ?: return null))
        log.debug("Address: $address")

        val output = txDTO.outputUtxos
            ?.firstOrNull {
                log.debug("Comparing: ${it.address}, ${Helpers.convertScriptAddressToHex(it.address)} to $address")
                Helpers.convertScriptAddressToHex(it.address) == address }?: return null

        val fee = lpInputDatumJsonNode.get("fields")?.get(7)?.get("int")?.asLong()?: return null
        log.debug("Fee: $fee")

        val amountOperationDTO = when (minswapSwapDirection == DexOperationEnum.BUY.code) {
            true -> {
                log.debug("ADA input: ${input.amounts.filter { it.unit == "lovelace" }}")
                val inputADA = input.amounts
                    .filter { it.unit == "lovelace" }
                    .map { it.quantity }
                    .reduceOrNull { a, b -> a.plus(b) }?: return null
                log.debug("Input ADA: $inputADA")
                val outputADA = output.amounts
                    .filter { it.unit == "lovelace" }
                    .map { it.quantity }
                    .reduceOrNull { a, b -> a.plus(b) }?: return null
                log.debug("Input ADA: $inputADA, outputADA: $outputADA")
                AmountOperationDTO(
                    inputADA.minus(outputADA).minus(BigInteger.valueOf(fee)),
                    output.amounts
                        .filter { it.unit.replace(".","") == asset2Unit}
                        .map { it.quantity }
                        .reduceOrNull { a, b -> a.plus(b) }?: return null,
                    DexOperationEnum.SELL.code
                )
            }
            false -> {
                AmountOperationDTO(
                    output.amounts
                        .filter { it.unit == "lovelace" }
                        .map { it.quantity }
                        .reduceOrNull { a, b -> a.plus(b) }?.minus(SWAP_OUT_ADA)?: return null,
                    input.amounts
                        .filter { it.unit.replace(".","") == asset2Unit}
                        .map { it.quantity }
                        .reduceOrNull { a, b -> a.plus(b) }?: return null,
                    DexOperationEnum.BUY.code
                )
            }
        }

        log.debug("Amounts, 1: ${amountOperationDTO.amount1}, 2: ${amountOperationDTO.amount2}, operation: ${amountOperationDTO.swapDirection}")
        if (amountOperationDTO.amount1<=BigInteger.ZERO || amountOperationDTO.amount2<=BigInteger.ZERO) {
            log.debug("Had a zero or negative amount, skipping...")
            return null
        }

        val swapDTO = SwapDTO(
            txDTO.txHash,
            txDTO.blockSlot,
            getDexCode(),
            asset1Unit,
            asset2Unit,
            amountOperationDTO.amount1.toBigDecimal(),
            amountOperationDTO.amount2.toBigDecimal(),
            amountOperationDTO.swapDirection
        )
        log.debug("Computed swap: $swapDTO")
        return swapDTO
    }

    fun computeZapIn(poolDatumJsonNode: JsonNode, lpInputDatumJsonNode: JsonNode, txDTO: FullyQualifiedTxDTO, asset1Unit: String, asset2Unit: String): SwapDTO? {
        val totalLiquidity = poolDatumJsonNode.get("fields")?.get(3)?.get("int")?.asLong()?.toBigInteger()?: return null
        val reserveIn = poolDatumJsonNode.get("fields")?.get(4)?.get("int")?.asLong()?.toBigInteger()?: return null
        val reserveOut = poolDatumJsonNode.get("fields")?.get(5)?.get("int")?.asLong()?.toBigInteger()?: return null
        val feeA = poolDatumJsonNode.get("fields")?.get(6)?.get("int")?.asLong()?.toBigInteger()?: return null
        val feeB = poolDatumJsonNode.get("fields")?.get(7)?.get("int")?.asLong()?.toBigInteger()?: return null

        val amountA = lpInputDatumJsonNode.get("fields")?.get(6)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("int")?.asLong()?.toBigInteger()?: return null
        val amountB = lpInputDatumJsonNode.get("fields")?.get(6)?.get("fields")?.get(0)?.get("fields")?.get(1)?.get("int")?.asLong()?.toBigInteger()?: return null

        val ratioA = (amountA * totalLiquidity) / reserveIn
        val ratioB = (amountB * totalLiquidity) / reserveOut

        val amountOperationDTO = when {
            ratioA > ratioB -> {
                // Swap A to B
                val (amount1, amount2) = calculateSwapAmounts(amountA, amountB, reserveIn, reserveOut, feeA)
                AmountOperationDTO(
                    amount1,
                    amount2,
                    DexOperationEnum.SELL.code
                )
            }
            ratioA < ratioB -> {
                // Swap B to A
                val (amount1, amount2) = calculateSwapAmounts(amountB, amountA, reserveOut, reserveIn, feeB)
                AmountOperationDTO(
                    amount2,
                    amount1,
                    DexOperationEnum.BUY.code
                )
            }
            else -> {
                return null
            }
        }

        log.debug("Amounts, 1: ${amountOperationDTO.amount1}, 2: ${amountOperationDTO.amount2}, operation: ${amountOperationDTO.swapDirection}")
        if (amountOperationDTO.amount1<=BigInteger.ZERO || amountOperationDTO.amount2<=BigInteger.ZERO) {
            log.debug("Had a zero or negative amount, skipping...")
            return null
        }

        val swapDTO = SwapDTO(
            txDTO.txHash,
            txDTO.blockSlot,
            getDexCode(),
            asset1Unit,
            asset2Unit,
            amountOperationDTO.amount1.toBigDecimal(),
            amountOperationDTO.amount2.toBigDecimal(),
            amountOperationDTO.swapDirection
        )
        log.debug("Computed swap: $swapDTO")
        return swapDTO
    }

    fun calculateSwapAmounts(amountA: BigInteger, amountB: BigInteger, reserveIn: BigInteger, reserveOut: BigInteger, tradingFee: BigInteger): Pair<BigInteger,BigInteger> {
        val x = (amountB + reserveOut) * reserveIn
        val y = BigInteger.valueOf(4) * (amountB + reserveOut) *(amountB * reserveIn * reserveIn - amountA * reserveIn * reserveOut)
        val z = BigInteger.valueOf(2) * (amountB + reserveOut)
        val a = MathUtils.pow(x, 2) * MathUtils.pow(BigInteger.valueOf(2) * DEFAULT_TRADING_FEE_DENOMINATOR - tradingFee, 2) -
                y * DEFAULT_TRADING_FEE_DENOMINATOR * (DEFAULT_TRADING_FEE_DENOMINATOR - tradingFee)
        val b = (BigInteger.valueOf(2) * DEFAULT_TRADING_FEE_DENOMINATOR - tradingFee) * x
        val numerator = sqrt(a, RoundingMode.FLOOR) - b
        val denominator = z * (DEFAULT_TRADING_FEE_DENOMINATOR - tradingFee)
        val amount1 = numerator/denominator
        /* Could compute lpAmount as; ((amountA * denominator - numerator) * totalLiquidity) / (reserveIn * denominator + numerator) */
        val amount2 = (DEFAULT_TRADING_FEE_DENOMINATOR-tradingFee) * amount1 * reserveOut / ( reserveIn *
                DEFAULT_TRADING_FEE_DENOMINATOR + (DEFAULT_TRADING_FEE_DENOMINATOR-tradingFee) * amount1)
        return Pair(amount1, amount2)
    }
}