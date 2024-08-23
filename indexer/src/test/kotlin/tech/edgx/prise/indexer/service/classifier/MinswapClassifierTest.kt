package tech.edgx.prise.indexer.service.classifier

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.junit.jupiter.api.Test
import tech.edgx.prise.indexer.model.DexOperationEnum
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.classifier.module.MinswapClassifier
import java.io.File
import java.math.BigInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MinswapClassifierTest {

    @Test
    fun computeSwap_SingleSwapSingleTransaction_1() {
        // RJV - ADA , Sell , 0.041 ADA , 11,341.315232 RJV , 465.245143 ADA , addr...mddj , 2024-01-01 19:21 GMT+8
        val swapTxHash = "79a56a719258ca13715bb82c24c869590c37859bba8329667a52f414950c13d1"
        val knownSwaps = listOf<Swap>(
            Swap(swapTxHash, 112541775, 2, "lovelace", "8cfd6893f5f6c1cc954cec1a0a1460841b74da6e7803820dde62bb78524a56", BigInteger.valueOf(465245143), BigInteger.valueOf(11341315232), DexOperationEnum.BUY.code),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>?>(
            File("src/test/resources/testdata/minswap/transactions_qualified_from_block_112541716.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO?>?>() {}.type)
            .filter { it.txHash == swapTxHash }
        val computedSwaps = MinswapClassifier.computeSwaps(txDTOs.first())
            .filter { it.dex == MinswapClassifier.DEX_CODE }
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
    fun computeSwap_SingleSwapSingleTransaction_2() {
        //SNEK - ADA , Buy , 0.00255 ADA , 1,789.513117 ADA , 701,549 SNEK , addr...mddj, 2024-01-01 22:36 GMT+8
        val swapTxHash = "b3e436dbe8af67247b1def8412fe172a905d98c106c08eb2ffe3c2fa91180c9d"
        val knownSwaps = listOf<Swap>(
            Swap(swapTxHash, 112553479, 2, "lovelace", "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b", BigInteger.valueOf(1789513117), BigInteger.valueOf(701549), 0),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>?>(
            File("src/test/resources/testdata/minswap/transactions_qualified_from_block_112553452.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO?>?>() {}.type)
            .filter { it.txHash == swapTxHash }
        val computedSwaps = MinswapClassifier.computeSwaps(txDTOs.first())
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