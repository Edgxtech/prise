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

object SundaeswapClassifier: DexClassifier {

    private val log = LoggerFactory.getLogger(javaClass)

    val DEX_CODE = 1
    val DEX_NAME = DexClassifierEnum.Sundaeswap.name
    val POOL_SCRIPT_HASH: String = "4020e7fc2de75a0729c3cc3af715b34d98381e0cdbcfa99c950bc3ac"
    val REQUEST_SCRIPT_HASH: String = "ba158766c1bae60e2117ee8987621441fac66a5e0fb9c7aca58cf20a"
    val SWAP_IN_ADA: BigInteger = BigInteger.valueOf(4500000)
    val SWAP_OUT_ADA: BigInteger = BigInteger.valueOf(2000000)
    val SS_LP_POLICY: String = "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913"

    override fun getDexCode(): Int {
        return DEX_CODE
    }

    override fun getDexName(): String {
        return DEX_NAME
    }

    override fun getPoolScriptHash(): List<String> {
        return listOf(POOL_SCRIPT_HASH)
    }

    /* Pass this tx with all inputs, outputs, witnesses */
    override fun computeSwaps(txDTO: FullyQualifiedTxDTO) : List<Swap> {
        log.debug("Computing swaps for txDTO: $txDTO")
        val swaps = mutableListOf<Swap>()

        /* qualified transactions are 'big hands' filtered, here we find the specific output to the dex and its datum */
        val outputDatumPair = txDTO.outputUtxos
            ?.firstOrNull {
                log.debug("Comparing output addr: ${it.address}, ${Helpers.convertScriptAddressToPaymentCredential(it.address)} to $POOL_SCRIPT_HASH")
                POOL_SCRIPT_HASH == Helpers.convertScriptAddressToPaymentCredential(it.address) }
            ?.let { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)?: return emptyList()
                log.debug("Found datum: ${datum.datumHash}")
                Pair(txOut, datum)
            }
        val lpDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(outputDatumPair?.second))?: return emptyList()
        log.debug("Extracted relevant output and datum: OUTPUT: ${outputDatumPair?.first} \n DATUM: ${lpDatumJsonNode}")

        val asset1Unit = when (lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.isEmpty) {
            true -> "lovelace"
            false -> lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus(// Policy
                    lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get("fields")?.get(0)?.get(1)?.get("bytes")?.asText()) // Name
                        ?: return listOf()
            null -> return emptyList() // Metadata doesn't contain the asset unit, skip
        }
        val asset2Unit = lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get(1)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                         lpDatumJsonNode.get("fields")?.get(0)?.get("fields")?.get(1)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                            ?: return listOf() // Metadata doesn't contain the asset unit, skip
        log.debug("Extracted asset 1: $asset1Unit, asset2 : $asset2Unit")

        /* Collected the dex lp inputs married to their corresponding datums */
        val inputDatumPairs = txDTO.inputUtxos
            .filter { REQUEST_SCRIPT_HASH == Helpers.convertScriptAddressToPaymentCredential(it.address) }
            .map { txOut ->
                val datum = ClassifierHelpers.getPlutusDataFromOutput(txOut, txDTO.witnesses.datums)
                Pair(txOut, datum)
            }
        log.debug("Number of input datum pairs: ${inputDatumPairs.size}, Number input utxos: ${txDTO.inputUtxos.size}")

        val utxoCandidates = txDTO.outputUtxos?.toMutableList()

        inputDatumPairs.forEach swaploop@{ (input, inputDatum) ->
            val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))
            log.debug("lpInputDatum: $lpDatumJsonNode")

            val ssOperation = lpInputDatumJsonNode?.get("fields")?.get(3)?.get("constructor")?.asInt()
            log.debug("Operation: $ssOperation")
            if (ssOperation != 0) {
                log.debug("Not a swap, skipping")
                return@swaploop
            }

            val swapDirection = lpInputDatumJsonNode.get("fields")?.get(3)?.get("fields")?.get(0)?.get("constructor")?.asInt()?: return@swaploop
            log.debug("Direction: $swapDirection")

            /* Find utxo corresponding to the swap input */
            val address = "01".plus(
                lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus(
                    lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()
                        ?: return@swaploop))
            log.debug("Address: $address")

            /* Match to outputs (note: can sometimes have duplicate inputs, but only one output, tracking candidates prevents classifying as two swaps) */
            val output = utxoCandidates?.firstOrNull {
                log.debug("Comparing: ${it.address}, ${Helpers.convertScriptAddressToHex(it.address)} to $address")
                Helpers.convertScriptAddressToHex(it.address) == address
            }?: return@swaploop
            utxoCandidates.remove(output)
            log.debug("utxo: $output")

            val (amount1, amount2) = when(swapDirection == DexOperationEnum.SELL.code) {
                true -> {
                    Pair(
                        input.amounts
                            .filter { it.unit == "lovelace" }
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?.minus(SWAP_IN_ADA)?: return@swaploop,
                        output.amounts
                            .filter { it.unit.replace(".","") == asset2Unit}
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?: return@swaploop,
                        )
                }
                false -> {
                    Pair(
                        output.amounts
                            .filter { it.unit == "lovelace" }
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?.minus(SWAP_OUT_ADA)?: return@swaploop,
                        input.amounts
                            .filter { it.unit.replace(".","") == asset2Unit}
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?: return@swaploop,
                        )
                }
            }
            log.debug("Amounts, 1: $amount1, 2: $amount2")

            val swap = Swap(
                txDTO.txHash,
                txDTO.blockSlot,
                DexEnum.SUNDAESWAP.code,
                asset1Unit,
                asset2Unit,
                amount1,
                amount2,
                swapDirection
            )
            log.debug("Computed swap: $swap")
            swaps.add(swap)
        }
        log.debug("Computed swaps: $swaps")
        return swaps
    }
}