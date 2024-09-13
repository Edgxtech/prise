package tech.edgx.prise.indexer.service.classifier

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.junit.jupiter.api.Test
import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.service.classifier.module.WingridersClassifier
import java.io.File
import java.math.BigInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WingridersClassifierTest {

    @Test
    fun computeSwap_SingleSwapSingleTransaction() {
        val swapTxHash = "ab5f019a744345179c3f80a55dec1c9b18266858a2fa09dcb57da7fd8b6cccab"
        val knownSwaps = listOf(
            Swap(swapTxHash, 112501933, 0, "lovelace", "51a5e236c4de3af2b8020442e2a26f454fda3b04cb621c1294a0ef34424f4f4b", BigInteger.valueOf(252819173), BigInteger.valueOf(5000000000), 1),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>>(
            File("src/test/resources/testdata/wingriders/transactions_swaps_from_block_112501875.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)
            /* This data actually has 4 qualifying tx, for this test only comparing the following */
            .filter { it.txHash == swapTxHash }
        println("# TXDTOS: ${txDTOs.map { it.txHash }}")
        val computedSwaps = WingridersClassifier.computeSwaps(txDTOs.first())
            .filter { it.dex == 0 }
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
    fun computeSwap_DuelSwapSingleTransaction() {
        val swapTxHash = "3be944381a57e86573be4cd0c888e831c7704d0b425f53b4685b3d522e624c5e"
        val knownSwaps = listOf(
            Swap(swapTxHash, 112587121, 0, "lovelace", "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d", BigInteger.valueOf(5391695121), BigInteger.valueOf(35000000000), 1),
            Swap(swapTxHash, 112587121, 0, "lovelace", "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0436f726e75636f70696173205b76696120436861696e506f72742e696f5d", BigInteger.valueOf(4050000000), BigInteger.valueOf(26169677478), 0),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson<List<FullyQualifiedTxDTO>>(
            File("src/test/resources/testdata/wingriders/transactions_qualified_from_block_112587104.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)
            .filter { it.txHash == swapTxHash }
        val computedSwaps = WingridersClassifier.computeSwaps(txDTOs.first())
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