package tech.edgx.prise.indexer.service.classifier

import com.bloxbean.cardano.client.plutus.spec.serializers.PlutusDataJsonConverter
import com.bloxbean.cardano.client.util.JsonUtil
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.junit.jupiter.api.Test
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.model.DexOperationEnum
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.SwapDTO
import tech.edgx.prise.indexer.service.classifier.common.ClassifierHelpers
import tech.edgx.prise.indexer.service.classifier.module.MinswapClassifier
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MinswapClassifierTest {

    @Test
    fun computeSwap_SingleSwapSingleTransaction_1() {
        // RJV - ADA , Sell , 0.041 ADA , 11,341.315232 RJV , 465.245143 ADA , addr...mddj , 2024-01-01 19:21 GMT+8
        // Note: Minswap Operation (Buy/Sell) is inverse of Prise Operation
        val swapTxHash = "79a56a719258ca13715bb82c24c869590c37859bba8329667a52f414950c13d1"
        val knownSwapDTOS = listOf(
            SwapDTO(swapTxHash, 112541775, 2, "lovelace", "8cfd6893f5f6c1cc954cec1a0a1460841b74da6e7803820dde62bb78524a56", BigDecimal.valueOf(465245143), BigDecimal.valueOf(11341315232), DexOperationEnum.BUY.code),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>>(
            File("src/test/resources/testdata/${DexEnum.MINSWAP.nativeName}/transactions_qualified_from_block_112541716.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)
            .filter { it.txHash == swapTxHash }
        val computedSwaps = MinswapClassifier.computeSwaps(txDTOs.first())
            .filter { it.dex == MinswapClassifier.DEX_CODE }
        assertEquals(knownSwapDTOS.size, computedSwaps.size)
        computedSwaps.zip(knownSwapDTOS).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwap_SingleSwapSingleTransaction_2() {
        // SNEK - ADA , Buy , 0.00255 ADA , 1,789.513117 ADA , 701,549 SNEK , addr...mddj, 2024-01-01 22:36 GMT+8
        val swapTxHash = "b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d"
        val knownSwapDTOS = listOf(
            SwapDTO(swapTxHash, 112553479, 2, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigDecimal.valueOf(1789513117), BigDecimal.valueOf(701549), DexOperationEnum.SELL.code),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>>(
            File("src/test/resources/testdata/${DexEnum.MINSWAP.nativeName}/transactions_qualified_from_block_112553452.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)
            .filter { it.txHash == swapTxHash }
        val computedSwaps = MinswapClassifier.computeSwaps(txDTOs.first())
        println("Computed swaps, #: ${computedSwaps.size} vs known swaps, #: ${knownSwapDTOS.size}")
        assertEquals(knownSwapDTOS.size, computedSwaps.size)
        knownSwapDTOS.zip(computedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwap_NilSwap_3() {
        // EXPECTING: Nil MinswapV1 Swaps, EVEN THOUGH THERE IS AN OUTPUT TO POOL CONTRACT
        val swapTxHash = "72875a21809e7c75d0e98e4751171eafc66847dc2614fa70208cfc067d565d90"
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAP.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(), FullyQualifiedTxDTO::class.java)
            //.filter { it.txHash == swapTxHash }
        val computedSwapDTOS: List<SwapDTO> = MinswapClassifier.computeSwaps(txDTO)
        println("Computed swaps, #: ${computedSwapDTOS.size}")
        assertTrue(computedSwapDTOS.isEmpty())
    }

    @Test
    fun computeSwap_SingleSwapSingleTransaction_4() {
        // AGIX Swap
        val swapTxHash = "b02042417e8fa4e2386b0e47c85e3f2d18e3483196c861767758ccba52e75730"
        val knownSwapDTOS = listOf(
            SwapDTO("b02042417e8fa4e2386b0e47c85e3f2d18e3483196c861767758ccba52e75730", 116978969L, DexEnum.MINSWAP.code, "lovelace", "f43a62fdc3965df486de8a0d32fe800963589c41b38946602a0dc53541474958", BigDecimal.valueOf(23193831393), BigDecimal.valueOf(2633400000000), DexOperationEnum.BUY.code)
        )
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAP.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(), FullyQualifiedTxDTO::class.java)
        val computedSwapDTOS: List<SwapDTO> = MinswapClassifier.computeSwaps(txDTO)
        println("Computed swaps, #: ${computedSwapDTOS.size}")
        knownSwapDTOS.zip(computedSwapDTOS).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwaps_SingleSwapSingleTransaction_5() {
        // WMT - ADA , Buy , 0.258 ADA , 270 ADA , 1,045.332878 WMT , addr...xuxp , 2024-01-01 08:09 GMT+8
        val swapTxHash = "3cae4bea2849f1cc8546a96058320f0deb949289cbd58295cfc7f50dab71f15a"
        val knownSwapDTOS = listOf(
            SwapDTO(swapTxHash, 112501470, DexEnum.MINSWAP.code, "lovelace", "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigDecimal.valueOf(270000000), BigDecimal.valueOf(1045332878), DexOperationEnum.SELL.code),
        )
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAP.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(), FullyQualifiedTxDTO::class.java)
        val computedSwapDTOS: List<SwapDTO> = MinswapClassifier.computeSwaps(txDTO)
        println("Computed swaps, #: ${computedSwapDTOS.size}")
        knownSwapDTOS.zip(computedSwapDTOS).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { it.first.amount2 == it.second.amount2 }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    companion object {
        /*
            Find any transactions of the given Minswap operation types
            0 = SWAP_EXACT_IN, 1 = SWAP_EXACT_OUT, 2 = DEPOSIT, 3 = WITHDRAW, 4 = ZapIn
         */

        fun filterSwapsByType(swapDTOS: List<SwapDTO>, txDTOs: List<FullyQualifiedTxDTO>, types: List<Int>): List<SwapDTO> {
            val filterMask = createSwapsFilterMasks(txDTOs, types)
            return swapDTOS.groupBy { it.txHash }
                .flatMap {
                    val masks: List<Boolean>? = filterMask[it.key]
                    val swaps = it.value
                    val retained = masks?.let {
                        swaps.zip(it).filter {
                            it.second
                        }.map { it.first }
                    }
                    retained?: emptyList()
                }
        }

        /* Needed a way to filter out unclassified types, here I create a mask to remove unwanted swaps within transactions */
        fun createSwapsFilterMasks(txDTOs: List<FullyQualifiedTxDTO>, types: List<Int>): Map<String,List<Boolean>> {
            val allMasks = mutableMapOf<String,List<Boolean>>()
            txDTOs.forEach { txDTO ->
                val inputUtxos = txDTO.inputUtxos.filter { MinswapClassifier.ORDER_CREDENTIALS_V1.contains(Helpers.convertScriptAddressToPaymentCredential(it.address)) }

                /* must sort by the output index IOT match the order from Minswap */
                val orderedInputUtxos = inputUtxos.sortedByDescending { inputUtxo ->
                    val inputDatum = ClassifierHelpers.getPlutusDataFromOutput(inputUtxo, txDTO.witnesses.datums)
                    val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))
                    val address = "01".plus(
                        lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus(
                            lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()))
                    val output = txDTO.outputUtxos?.firstOrNull { Helpers.convertScriptAddressToHex(it.address) == address }
                    val outputIdx = txDTO.outputUtxos?.indexOf(output)
                    outputIdx
                }

                /* Create mask based on whether the swap is desired as per "types" */
                val swapMask = orderedInputUtxos.map { inputUtxo ->
                    val inputDatum = ClassifierHelpers.getPlutusDataFromOutput(inputUtxo, txDTO.witnesses.datums)
                    val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))
                    //val minswapOperation = lpInputDatumJsonNode?.get("fields")?.get(6)?.get("constructor")?.asInt()
                    val minswapOperation = lpInputDatumJsonNode?.get("fields")?.get(3)?.get("constructor")?.asInt()
                    Pair(txDTO.txHash, types.contains(minswapOperation))
                }.groupBy(Pair<String, Boolean>::first, Pair<String, Boolean>::second)

                //println("For TX: ${txDTO.txHash}, swapMasks: ${swapMask.map { it.value }}")
                allMasks.putAll(swapMask)
            }
            return allMasks
        }
    }
}