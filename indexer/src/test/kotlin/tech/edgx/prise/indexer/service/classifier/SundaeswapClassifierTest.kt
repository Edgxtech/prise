package tech.edgx.prise.indexer.service.classifier

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.junit.jupiter.api.Test
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.classifier.module.SundaeswapClassifier
import java.io.File
import java.math.BigInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SundaeswapClassifierTest {

    @Test
    fun computeSwap_SingleSwapSingleTransaction() {
        val swapTxHash = "0dd5f01748068cd6a3a65273159447bf38a9c78de04f28e4f8ad0465d384af6c"
        val knownSwaps = listOf(
            Swap(swapTxHash, 112504002, 1, "lovelace", "682fe60c9918842b3323c43b5144bc3d52a23bd2fb81345560d73f634e45574d", BigInteger.valueOf(30556170), BigInteger.valueOf(2056039876), 0),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>>(
            File("src/test/resources/testdata/sundaeswap/transactions_qualified_from_block_112503994.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)
            .filter { it.txHash == swapTxHash }
        val computedSwaps = SundaeswapClassifier.computeSwaps(txDTOs.first())
            .filter { it.dex == SundaeswapClassifier.DEX_CODE }
        assertEquals(knownSwaps.size, computedSwaps.size)
        computedSwaps.zip(knownSwaps).forEach {
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
    fun computeSwap_TripleSwapSingleTransaction() {
        val swapTxHash = "589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3"
        val knownSwaps = listOf(
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(150000000), BigInteger.valueOf(565416591383), 0),
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(99021655), BigInteger.valueOf(338406672751), 0),
            Swap("589f59c70c314fca7fd9d8248d89cc9a16cdedd8dde2cfc6689e0b2bae84a6d3", 112502077, 1, "lovelace", "94cbb4fcbcaa2975779f273b263eb3b5f24a9951e446d6dc4c13586452455655", BigInteger.valueOf(165772912), BigInteger.valueOf(585170875225), 1)
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>?>(
            File("src/test/resources/testdata/sundaeswap/transactions_qualified_from_block_112501957.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO?>?>() {}.type)
            .filter { it.txHash == swapTxHash }
        val computedSwaps = SundaeswapClassifier.computeSwaps(txDTOs.first())
        println("Computed swaps, #: ${computedSwaps.size} vs known swaps, #: ${knownSwaps.size}")
        assertEquals(knownSwaps.size, computedSwaps.size)
        computedSwaps.zip(knownSwaps).forEach {
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
}