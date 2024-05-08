package tech.edgx.prise.indexer.service.classifier.module

import co.nstant.`in`.cbor.model.Array
import com.bloxbean.cardano.client.common.cbor.CborSerializationUtil
import com.bloxbean.cardano.client.plutus.spec.PlutusData
import com.bloxbean.cardano.client.plutus.spec.Redeemer
import com.bloxbean.cardano.client.plutus.spec.serializers.PlutusDataJsonConverter
import com.bloxbean.cardano.client.util.JsonUtil
import com.bloxbean.cardano.yaci.core.util.HexUtil
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.model.DexOperationEnum
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.classifier.common.ClassifierHelpers
import tech.edgx.prise.indexer.service.classifier.common.DexClassifierEnum
import java.math.BigInteger

object WingridersClassifier: DexClassifier {

    private val log = LoggerFactory.getLogger(javaClass)

    val DEX_CODE = 0
    val DEX_NAME = DexClassifierEnum.Wingriders.name
    val POOL_SCRIPT_HASH: String = "e6c90a5923713af5786963dee0fdffd830ca7e0c86a041d9e5833e91"
    val SWAP_IN_ADA_BASE: BigInteger = BigInteger.valueOf(2000000)
    val FEE_CHANGE_SLOT = 117994509L // (04.03.2024 14:00:00 UTC)
    val LOWER_FEE_LIMIT = BigInteger.valueOf(250000000)
    val UPPER_FEE_LIMIT = BigInteger.valueOf(500000000)
    val LOW_FEE = BigInteger.valueOf(850000)
    val MID_FEE = BigInteger.valueOf(1500000)
    val HIGH_FEE = BigInteger.valueOf(2000000)

    val SWAP_OUT_ADA: BigInteger = BigInteger.valueOf(2000000)
    val WR_LP_POLICY: String = "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570"

    override fun getDexCode(): Int {
        return DEX_CODE
    }

    override fun getDexName(): String {
        return DEX_NAME
    }

    override fun getPoolScriptHash(): List<String> {
        return listOf(POOL_SCRIPT_HASH)
    }

    override fun computeSwaps(txDTO: FullyQualifiedTxDTO) : List<Swap> {
            val swaps = mutableListOf<Swap>()

            /* The first redeemer of the transaction contains index of other redeemer for the liquidity pool input */
            val lpInputRedeemerIdx: Int = txDTO.witnesses.redeemers
                .firstOrNull()
                ?.let {
                    log.debug("redeemer cbor: ${it.cbor}")
                    val redeemerJson = PlutusDataJsonConverter.toJson(
                        Redeemer.deserialize(
                            CborSerializationUtil.deserialize(
                                HexUtil.decodeHexString(it.cbor)) as Array
                        ).data)
                    log.debug("Pool input redeemer plutusdata: $redeemerJson")
                    JsonUtil.parseJson(redeemerJson)?.get("fields")?.get(0)?.get("int")?.asInt()?: return listOf()
                }?: return emptyList()

            // Find redeemer used with the lp input - it contains swap indexes to map swapped inputs to outputs
            val lpInputRedeemer = txDTO.witnesses.redeemers
                .map { Redeemer.deserialize(CborSerializationUtil.deserialize(HexUtil.decodeHexString(it.cbor)) as Array) }
                .firstOrNull() {
                    log.debug("Index (internal to redeemer): ${it.index}")
                    // Need to actually decode the redeemer data, to compare its internal index
                    it.index.toInt() == lpInputRedeemerIdx }?: return emptyList()

            val lpInputRedeemerDataNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(lpInputRedeemer.data))?: return emptyList()
            log.debug("lpInput Redeemer data: $lpInputRedeemerDataNode")

            /* Extract swaps input list from lp Input Redeemer Data, method used by WR to map swap inputs to outputs */
            val swapIndexes = lpInputRedeemerDataNode.get("fields")?.get(2)?.get("list")
                ?.map { it.get("int").asInt() }
                ?: return listOf()
            log.debug("Swap indexes: $swapIndexes")

            // Find main datum - obtained from the lpInput redeemer as the index to the input containing the transaction level datum
            val lpInputIndex = lpInputRedeemerDataNode.get("fields")?.get(0)?.get("int")?.asInt()?: return listOf()
            log.debug("Index to main lp input: $lpInputIndex")

            /* get information about swap from lp plutus data - contained at the input to the pool */
            val inputs = txDTO.inputUtxos
            log.debug("Input[parent] datum hash: ${inputs[lpInputIndex].datumHash}")
            val lpDatum: PlutusData = ClassifierHelpers.getPlutusDataFromOutput(
                inputs[lpInputIndex],
                txDTO.witnesses.datums
            ) ?: return emptyList()
            val lpDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(lpDatum))
            log.debug("MainDatum: $lpDatumJsonNode")

            val asset1Unit = when(lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.isEmpty) {
                true -> "lovelace"
                false -> lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                         lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                            ?: return listOf()
                null -> return listOf()
            }
            val asset2Unit = lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(1)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus( // Policy
                             lpDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(1)?.get("fields")?.get(1)?.get("bytes")?.asText()) // Name
                                ?: return listOf()
            log.debug("Extracted asset 1: $asset1Unit, asset2 : $asset2Unit")

            txDTO.outputUtxos?.drop(1)?.zip(swapIndexes)?.map swaploop@{ outputSwapIdxPair ->

                /* Match input for the swap, is specified by the index provided in redeemer */
                log.debug("Index of input: ${outputSwapIdxPair.second}")
                val output = outputSwapIdxPair.first
                val input = inputs[outputSwapIdxPair.second]
                log.debug("Output: $output, Input: $input")

                /* The input datum contains detail about the operation (buy or sell) */
                val inputDatum: PlutusData = ClassifierHelpers.getPlutusDataFromOutput(input, txDTO.witnesses.datums) ?: return@swaploop
                val inputDatumJsonNode = JsonUtil.parseJson(
                    PlutusDataJsonConverter.toJson(
                    inputDatum
                ))
                log.debug("InputDatum: $inputDatumJsonNode")

                val wrOperation = inputDatumJsonNode.get("fields")?.get(1)?.get("constructor")?.asInt()
                if (wrOperation != 0) {
                    log.debug("Not a swap, skipping")
                    return@swaploop
                }

                val swapDirection = inputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("constructor")?.asInt()?: return@swaploop
                log.debug("Operation: $wrOperation, Direction: $swapDirection")

                val (amount1, amount2) = when(swapDirection == DexOperationEnum.SELL.code) {
                    true -> {
                        val adaAmount = input.amounts
                            .filter { it.unit == "lovelace" }
                            .map { it.quantity }
                            .reduceOrNull { a, b -> a.plus(b) }?: return@swaploop
                        log.debug("Block slot: ${txDTO.blockSlot}, ada amount: $adaAmount")
                        val SWAP_IN_ADA = getFees()
                        Pair(
                            adaAmount.minus(SWAP_IN_ADA),
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
                    DexEnum.WINGRIDERS.code,
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

    /* Original method */
    fun getFees(): BigInteger {
        return SWAP_IN_ADA_BASE.plus(HIGH_FEE)
    }

    /* Investigating new fee structure */
    fun getTimeSpecificFees(adaAmount: BigInteger, slot: Long): BigInteger? {
        return when (slot < FEE_CHANGE_SLOT) {
            true -> {
                log.debug("LESS THAN FEE CHANGE TIME")
                SWAP_IN_ADA_BASE.plus(HIGH_FEE)
            }
            false -> {
                when {
                    (adaAmount <= LOWER_FEE_LIMIT) -> {
                        log.debug("LOWER FEE: $LOW_FEE")
                        SWAP_IN_ADA_BASE.plus(LOW_FEE)
                    }
                    (adaAmount > LOWER_FEE_LIMIT && adaAmount <= UPPER_FEE_LIMIT) -> {
                        log.debug("MID FEE: $MID_FEE")
                        SWAP_IN_ADA_BASE.plus(MID_FEE)
                    }
                    (adaAmount > UPPER_FEE_LIMIT) -> {
                        log.debug("HIGH FEE: $HIGH_FEE")
                        SWAP_IN_ADA_BASE.plus(HIGH_FEE)
                    }
                    else -> return null
                }
            }
        }
    }
}