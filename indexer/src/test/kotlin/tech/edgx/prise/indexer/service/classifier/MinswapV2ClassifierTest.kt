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
import tech.edgx.prise.indexer.service.classifier.module.MinswapV2Classifier
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.math.BigDecimal
import java.util.ArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MinswapV2ClassifierTest {

    @Test
    fun computeSwap_SingleTransaction_SingleSwap_1() {
        val swapTxHash = "b5eba3bc2628102ec30d7511cdb3d4a29ba506cbda844671e1ece0dfa896ecfe"
        val knownSwapDTOS = listOf(
            //DEDI/ADA Swap, Buy 0.192 ADA 200 ADA 1,041.2254 DEDI addr...0mnq 2024-08-16 08:46 GMT+8
            SwapDTO(swapTxHash, 132202912, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(200000000), BigDecimal.valueOf(1041225400), DexOperationEnum.SELL.code),
        )
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$swapTxHash.json")
            .readText(Charsets.UTF_8)
            .byteInputStream()
            .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val computedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }

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
    fun computeSwap_SingleTransaction_SingleSwap_2() {
        val swapTxHash = "269a8408bb1d47087a164267fcc6488dd65754d31b9c0f1547e63d6850ed35a4"
        //DEDI - ADA, Sell, 0.192 ADA, 13,540.519741 DEDI, 2,605.777417 ADA, addr...n73g , 2024-08-16 10:27 GMT+8
        val knownSwapDTOS = listOf(
            SwapDTO(swapTxHash, 132208958, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(2605777417), BigDecimal.valueOf(13540519741), DexOperationEnum.BUY.code),
        )
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val computedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }

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
    fun computeSwap_SingleTransaction_SingleSwap_3() {
        val swapTxHash = "f01615dc120442a43d08dbcd13c80044aafbde1ddb7c7e8899e3222e3a556858"
        val knownSwapDTOS = listOf(
            SwapDTO("f01615dc120442a43d08dbcd13c80044aafbde1ddb7c7e8899e3222e3a556858", 132216341, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(2061343905), BigDecimal.valueOf(9956660000), DexOperationEnum.BUY.code),
            SwapDTO("f01615dc120442a43d08dbcd13c80044aafbde1ddb7c7e8899e3222e3a556858", 132216341, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(9862068966), BigDecimal.valueOf(48129095766), DexOperationEnum.SELL.code),
        )
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedKnownSwaps = knownSwapDTOS.sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
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
    fun computeSwap_SingleTransaction_SingleSwap_4() {
        val txHash = "fa30415a3d5e336b80d940d880461bcf19cd1c63b2981ccbe51c1f30557feb82"
        val knownSwapDTOS = listOf(
            SwapDTO(txHash, 132239546, DexEnum.MINSWAPV2.code, "lovelace", "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e516f7263666178746f6b656e", BigDecimal.valueOf(129610777 ), BigDecimal.valueOf(5000000000), DexOperationEnum.BUY.code),
        )
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$txHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val orderedKnownSwaps = knownSwapDTOS
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        assertEquals(orderedKnownSwaps.size,orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
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

    /* type==4; DEPOSIT (i.e. ZapIn) */
    @Test
    fun computeSwaps_ZapIn() {
        val txHash = "d3aba39861706b25c5a8c33ce48889be998405a55b646002ee2f085e5a9fcd14"
        val knownSwapDTOS = listOf(
            // Orcfax Token, Buy , 0.026 ADA , 42.56296 ADA , 1,631.976871 FACT , addr...xpw0 , 2024-08-16 14:08 GMT+8
            SwapDTO(txHash, 132222216, DexEnum.MINSWAPV2.code, "lovelace", "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e516f7263666178746f6b656e", BigDecimal.valueOf(42562960 ), BigDecimal.valueOf(1631976871 ), DexOperationEnum.SELL.code),
        )
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$txHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val orderedKnownSwaps = filterSwapsByType(knownSwapDTOS, listOf(txDTO), listOf(0, 4))
            .filter { swapAssetUnits.contains(it.asset2Unit)  }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { swapAssetUnits.contains(it.asset2Unit)  }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        assertEquals(orderedKnownSwaps.size,orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            /* Discussions with Minswap confirmed the equations used are correct however the values taken from their
               history page are v.slightly off and they may have roundings errors on UI, here we only check if the values are close */
            assertTrue { (it.first.amount2 - it.second.amount2).abs() < BigDecimal.valueOf(10) }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    /* type==4; DEPOSIT (i.e. ZapIn) however has a strange condition where amount2 resolves as 0 thus is correctly skipped */
    @Test
    fun computeSwap_ZapIn_2() {
        val swapTxHash = "505bd29029f181a40f2e6d6c59a3628086d6161c358a59e08f0d253b90f8097b"
        val knownSwapDTOS: List<SwapDTO> = listOf()
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()

        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val orderedKnownSwaps = knownSwapDTOS
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }

        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
    }

    @Test
    fun computeSwap_ZapIn_3() {
        val swapTxHash = "5a164d5248026e39dc14912eab0434af786001a2c38674c5a34e8cb601abd204"
        val knownSwapDTOS = listOf(
            // CCCC - ADA, Sell , 0.07251 ADA, 11,952,952 CCCC, 0.301078 ADA , addr...8qp2 , 2024-11-21 10:51 GMT+8
            SwapDTO(txHash=swapTxHash, slot=140591196, dex=3, asset1Unit="lovelace", asset2Unit="ab3e31c490d248c592d5bb495823a45fd10f9c8e4f561f13551803fb43617264616e6f20436f6d6d756e697479204368617269747920436f696e", amount1=BigDecimal.valueOf(301078), amount2=BigDecimal.valueOf(11952952), operation=DexOperationEnum.BUY.code)
        )
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()

        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val orderedKnownSwaps = knownSwapDTOS
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }

        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
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

    /* Example of ZapIn with two non-zero input amounts */
    @Test
    fun computeSwap_ZapIn_4() {
        val swapTxHash = "f4d58892c029fd778982ea87c66a07c713c5a4975bbc1855ef637c143fc1553c"
        val knownSwapDTOS: List<SwapDTO> = listOf(
            //WMT - ADA, Buy, 0.503 ADA, 0.242164 ADA, 0.481323 WMT, addr...wyez, 2024-08-01 08:52 GMT+8
            SwapDTO(swapTxHash, 130907278L, DexEnum.MINSWAPV2.code, "lovelace", "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigDecimal.valueOf(242164), BigDecimal.valueOf(481323), DexOperationEnum.SELL.code),
        )
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()

        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java
        )

        val orderedKnownSwaps = knownSwapDTOS
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }

        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            assertTrue { (it.first.amount2 - it.second.amount2).abs() < BigDecimal.valueOf(10) }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    @Test
    fun computeSwap_SingleTransaction_SingleSwap_Type3_SwapExactOut() {
        //DEDI - ADA , Buy , 0.197 ADA , 197.129582 ADA , 1,000 DEDI , addr...jl93
        val swapTxHash = "592ac794f7ea60aba502b44aa436178626bef087ce7f05bf4a53dbd1456287be"
        val knownSwapDTOS = listOf(
            /* This gets filtered out since is type==3; SWAP_EXACT_OUT */
            SwapDTO(swapTxHash, 132219880, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(197129582), BigDecimal.valueOf(1000000000), DexOperationEnum.SELL.code),
        )
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$swapTxHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedKnownSwaps = filterSwapsByType(knownSwapDTOS, listOf(txDTO), listOf(0))
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        assertEquals(orderedKnownSwaps.size, orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
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
    fun computeSwaps_SingleTransaction_QtySwaps_Type3_SwapExactOut() {
        val txHash = "24983065abb54ff66368fda2c32372325bac4a1320452fd7643210699e76c6ae"
        val knownSwapDTOS = listOf(
            SwapDTO(txHash, 130934066, DexEnum.MINSWAPV2.code, "lovelace", "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigDecimal.valueOf(17523004082 ), BigDecimal.valueOf(35449239058 ), DexOperationEnum.SELL.code),
            SwapDTO(txHash, 130934066, DexEnum.MINSWAPV2.code, "lovelace", "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigDecimal.valueOf(2250000000 ), BigDecimal.valueOf(4586658642 ), DexOperationEnum.SELL.code),
            /* This gets filtered out since is type==3; SWAP_EXACT_OUT */
            SwapDTO(txHash, 130934066, DexEnum.MINSWAPV2.code, "lovelace", "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigDecimal.valueOf(35000000000 ), BigDecimal.valueOf(70874166564 ), DexOperationEnum.BUY.code),
        )
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()
        val txDTO: FullyQualifiedTxDTO = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transaction_qualified_$txHash.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, FullyQualifiedTxDTO::class.java)

        val orderedKnownSwaps = filterSwapsByType(knownSwapDTOS, listOf(txDTO), listOf(0))
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = MinswapV2Classifier.computeSwaps(txDTO)
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing # known swaps: ${orderedKnownSwaps.size} vs computed swaps: ${orderedComputedSwaps.size}")
        assertEquals(orderedKnownSwaps.size,orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
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
    fun computeSwaps_QtyTransaction_QtySwaps_1() {
        /* Qty DEDI/ADA swaps */
        val SWAP_ASSET_UNIT = "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449"
        val knownSwapDTOS = listOf(
            SwapDTO("14f20e4a633f81ff41904d580bf6837f9a09b6cc6864951fe53c133a67b2a3c3", 132220815, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(510283767), BigDecimal.valueOf(2619000000), DexOperationEnum.BUY.code),
            SwapDTO("94cd847fca66c2953645817ec461c55c2d5e4ae3c6d3c2f9d54576a2e45873ba", 132220695, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(977832974), BigDecimal.valueOf(5000000000), DexOperationEnum.BUY.code),
            SwapDTO("592ac794f7ea60aba502b44aa436178626bef087ce7f05bf4a53dbd1456287be", 132219880, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(197129582), BigDecimal.valueOf(1000000000), DexOperationEnum.SELL.code),
            SwapDTO("069b365ef33ed07bb087682d3a1f5cb0f3494fad8daa753a7c39ccc464539f95", 132219194, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(1000000000), BigDecimal.valueOf(5087991254), DexOperationEnum.SELL.code),
            SwapDTO("f37ed76e1466489d833e335ffba1b27b578e1ed86da7c6bf391947318a0a6814", 132218522, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(100000000), BigDecimal.valueOf(510177661), DexOperationEnum.SELL.code),
            SwapDTO("67afe34975df81820e4599d4a3f1f2776c4debdcffcfb95f15b61fa039a4c56b", 132218404, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(2000000000), BigDecimal.valueOf(10249480041), DexOperationEnum.SELL.code),
            SwapDTO("86cf291baa9d8341d2ef246cd00c4d4333a657c6a7bbd32789bbaa00985b80bb", 132218278, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(4200682701), BigDecimal.valueOf(21555091558), DexOperationEnum.BUY.code),
            SwapDTO("f5bda63b84377e00165376e82a1b904f7a126f9f9c1723fa5f5ac42d5621b278", 132218086, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(5123289763), BigDecimal.valueOf(25773195876), DexOperationEnum.BUY.code),
            SwapDTO("6724d44b03ceb59324db8ecaafcd7f4dc2b9e6cd0bc6caa67152917e41d169c5", 132217553, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(31910891), BigDecimal.valueOf(158790780), DexOperationEnum.BUY.code),
            SwapDTO("695de5db3cebeed45d563420433490b61312807436e1e0b440835d7a11a2f3dd", 132217469, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(318685018), BigDecimal.valueOf(1584627648), DexOperationEnum.BUY.code),
            SwapDTO("a4522d210cbf55f4ebf13c538e32785bea2a10257937db526994c1cfd4f012a9", 132217188, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(452563103), BigDecimal.valueOf(2246673856), DexOperationEnum.BUY.code),
            SwapDTO("3932955b1512124deee152f55e844c20ee6903e37bca597576530eb27457d546", 132216619, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(5302139120), BigDecimal.valueOf(26006408598), DexOperationEnum.BUY.code),
            SwapDTO("f01615dc120442a43d08dbcd13c80044aafbde1ddb7c7e8899e3222e3a556858", 132216341, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(2061343905), BigDecimal.valueOf(9956660000), DexOperationEnum.BUY.code),
            SwapDTO("f01615dc120442a43d08dbcd13c80044aafbde1ddb7c7e8899e3222e3a556858", 132216341, DexEnum.MINSWAPV2.code, "lovelace", "64f7b108bd43f4bde344b82587655eeb821256c0c8e79ad48db15d1844454449", BigDecimal.valueOf(9862068966), BigDecimal.valueOf(48129095766), DexOperationEnum.SELL.code),
        )
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transactions_qualified_qty_swaps_1.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)

        val orderedKnownSwaps = filterSwapsByType(knownSwapDTOS, txDTOs, listOf(0))
            .filter { it.asset2Unit==SWAP_ASSET_UNIT }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = txDTOs.flatMap { MinswapV2Classifier.computeSwaps(it) }
            .filter { it.asset2Unit==SWAP_ASSET_UNIT }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing # known swaps: ${orderedKnownSwaps.size} vs computed swaps: ${orderedComputedSwaps.size}")
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
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
    fun computeSwaps_QtyTransaction_QtySwaps_2() {
        val knownSwapDTOS = listOf(
            // Sell, 0.0888 ADA, 5,203.44246 NTX, 462.216162 ADA, addr...36p5, 2024-08-17 08:54 GMT+8
            SwapDTO("92da9170498a95b7e403e3ef60ae89d2dad8535df0a547b2fd54363d17b5dc55", 132289761, DexEnum.MINSWAPV2.code, "lovelace", "edfd7a1d77bcb8b884c474bdc92a16002d1fb720e454fa6e993444794e5458", BigDecimal.valueOf(462216162 ), BigDecimal.valueOf(5203442460 ), DexOperationEnum.BUY.code),
            // Buy, 0.0893 ADA, 14.278626 ADA, 159.734776 NTX, addr...ympe, 2024-08-17 01:14 GMT+8
            SwapDTO("06759669c3ee355e72702949ad93f94e71477eefd075139cad6fdafd8ff8eaae", 132262197, DexEnum.MINSWAPV2.code, "lovelace", "edfd7a1d77bcb8b884c474bdc92a16002d1fb720e454fa6e993444794e5458", BigDecimal.valueOf(14278626 ), BigDecimal.valueOf(159734776 ), DexOperationEnum.SELL.code),
            // Note: this txHash=505bd29029f181a40f2e6d6c59a3628086d6161c358a59e08f0d253b90f8097b qualifies as a ZapIn, however during amount calculations returns a zero and is correctly skipped
        )
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transactions_qualified_qty_swaps_2.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)

        val orderedKnownSwaps = knownSwapDTOS
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = txDTOs.flatMap { MinswapV2Classifier.computeSwaps(it) }
            .filter { swapAssetUnits.contains(it.asset2Unit)  }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        println("Comparing # known swaps: ${orderedKnownSwaps.size} vs computed swaps: ${orderedComputedSwaps.size}")
        println("COMPUTED SWAPS: $orderedComputedSwaps")
        assertEquals(orderedKnownSwaps.size,orderedComputedSwaps.size)
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
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
    fun computeSwaps_QtyTransaction_QtySwaps_3() {
        val knownSwapDTOS = listOf(
            SwapDTO("fa30415a3d5e336b80d940d880461bcf19cd1c63b2981ccbe51c1f30557feb82", 132239546, DexEnum.MINSWAPV2.code, "lovelace", "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e516f7263666178746f6b656e", BigDecimal.valueOf(129610777 ), BigDecimal.valueOf(5000000000), DexOperationEnum.BUY.code),
            /* This is contained in the tx qualified transaction dataset but is of type==4; DEPOSIT */
            SwapDTO("d3aba39861706b25c5a8c33ce48889be998405a55b646002ee2f085e5a9fcd14", 132222216, DexEnum.MINSWAPV2.code, "lovelace", "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e516f7263666178746f6b656e", BigDecimal.valueOf(42562960 ), BigDecimal.valueOf(1631976871 ), DexOperationEnum.SELL.code),
        )
        val swapAssetUnits = knownSwapDTOS.map { it.asset2Unit }.toSet()
        val txDTOs: List<FullyQualifiedTxDTO> = Gson().fromJson(
            File("src/test/resources/testdata/${DexEnum.MINSWAPV2.nativeName}/transactions_qualified_qty_swaps_3.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().use { it.readText() }, object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)

        val orderedKnownSwaps = filterSwapsByType(knownSwapDTOS, txDTOs, listOf(0, 4))
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }

        val orderedComputedSwaps = txDTOs.flatMap { MinswapV2Classifier.computeSwaps(it) }.asSequence()
            .filter { it.dex == MinswapV2Classifier.DEX_CODE }
            .filter { swapAssetUnits.contains(it.asset2Unit) }
            .sortedBy { it.txHash }.sortedBy { it.amount1 }.sortedBy { it.amount2 }.toList()

        println("Comparing # known swaps: ${orderedKnownSwaps.size} vs computed swaps: ${orderedComputedSwaps.size}")
        orderedKnownSwaps.zip(orderedComputedSwaps).forEach {
            println("Comparing: ${it.first} to ${it.second}")
            assertTrue { it.first.txHash == it.second.txHash }
            assertTrue { it.first.slot == it.second.slot }
            assertTrue { it.first.dex == it.second.dex }
            assertTrue { it.first.asset1Unit == it.second.asset1Unit }
            assertTrue { it.first.asset2Unit == it.second.asset2Unit }
            assertTrue { it.first.amount1 == it.second.amount1 }
            when (it.first.txHash == "d3aba39861706b25c5a8c33ce48889be998405a55b646002ee2f085e5a9fcd14") {
                true -> assertTrue { (it.first.amount2 - it.second.amount2).abs() < BigDecimal.valueOf(10) }
                else -> assertTrue { it.first.amount2 == it.second.amount2 }
            }
            assertTrue { it.first.operation == it.second.operation }
        }
    }

    companion object {
        /*
            Find any transactions of the given Minswap operation types
                SWAP_EXACT_IN = 0,
                STOP,
                OCO,
                SWAP_EXACT_OUT,
                DEPOSIT,
                WITHDRAW,
                ZAP_OUT,
                PARTIAL_SWAP,
                WITHDRAW_IMBALANCE,
                SWAP_ROUTING,
                DONATION,
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
                val inputUtxos = txDTO.inputUtxos.filter { MinswapV2Classifier.ORDER_SCRIPT_HASHES.contains(Helpers.convertScriptAddressToPaymentCredential(it.address)) }

                /* must sort by the output index IOT match the order from Minswap */
                val orderedInputUtxos = inputUtxos.sortedByDescending { inputUtxo ->
                    val inputDatum = ClassifierHelpers.getPlutusDataFromOutput(inputUtxo, txDTO.witnesses.datums)
                    val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))
                    val address = "01".plus(
                        lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()?.plus(
                            lpInputDatumJsonNode.get("fields")?.get(1)?.get("fields")?.get(1)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()
                        ))
                    val output = txDTO.outputUtxos?.firstOrNull { Helpers.convertScriptAddressToHex(it.address) == address }
                    val outputIdx = txDTO.outputUtxos?.indexOf(output)
                    outputIdx
                }

                /* Create mask based on whether the swap is desired as per "types" */
                val swapMask = orderedInputUtxos.map { inputUtxo ->
                    val inputDatum = ClassifierHelpers.getPlutusDataFromOutput(inputUtxo, txDTO.witnesses.datums)
                    val lpInputDatumJsonNode = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(inputDatum))
                    val minswapOperation = lpInputDatumJsonNode?.get("fields")?.get(6)?.get("constructor")?.asInt()
                    Pair(txDTO.txHash, types.contains(minswapOperation))
                }.groupBy(Pair<String, Boolean>::first, Pair<String, Boolean>::second)

                //println("For TX: ${txDTO.txHash}, swapMasks: ${swapMask.map { it.value }}")
                allMasks.putAll(swapMask)
            }
            return allMasks
        }
    }
}