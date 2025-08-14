package tech.edgx.prise.indexer.service.chaindb

import com.bloxbean.cardano.yaci.core.model.TransactionInput
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.test.inject
import tech.edgx.prise.indexer.Base
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import java.math.BigInteger
import java.util.*
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BlockfrostServiceIT: Base() {

    val blockfrostService: ChainDatabaseService by inject(named("blockfrost")) { parametersOf(config) }

    @Test
    fun getInputUtxos() {
        val txIns = setOf(
            TransactionInput("6af1e37d864b8dfb3c23a641e46ac88dfce35f716bd6d45c4513cebe4466cae8", 2),
            TransactionInput("a419541b8306d10cab64283026a70d50192f562b9e00f795eaa9a14239f44484", 0),
            TransactionInput("f138b715fad2facf65dbc1ff0f69c715a6429e1739af48c276bbbcd422f1eabf", 0)
        )
        val txOuts = runBlocking { blockfrostService.getInputUtxos(txIns) }
        println("Retrieved txIn details, #: ${txOuts.size}, $txOuts")
        assertTrue(txOuts.isNotEmpty())
        assertTrue(txOuts[0].address.equals("addr1q87hqhxpdat6du2m4tzgkrftn89zdnmu2ql2fpmuwqe85zs4mrjqlk30ekjfdanrl04k6fc4xnhmnpz9lnmhge8wa77s7p0ukd"))
        assertTrue(txOuts[0].amounts.filter { it.unit == "lovelace" }.map { it.quantity }.first().equals(BigInteger.valueOf(136790130L)))
        assertTrue(txOuts[0].amounts.size.equals(2))
        assertTrue(txOuts[0].datumHash == null)
        assertTrue(txOuts[0].inlineDatum == null)
        assertTrue(txOuts[0].scriptRef == null)
        assertTrue(txOuts[1].address.equals("addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytvcg6zm2vds6mz9x3h3yalqmnf24w86m09n40q3tgqxjms9yu6v8"))
        assertTrue(txOuts[1].amounts.filter { it.unit == "lovelace" }.map { it.quantity }.first().equals(BigInteger.valueOf(1184604196621L)))
        assertTrue(txOuts[1].amounts.size.equals(4))
        assertTrue(txOuts[1].datumHash.equals("7cdbffb50b4944987aefadf01736fd8e0fc2dafc3b6287e0af165735bfbe8887"))
        assertTrue(txOuts[1].inlineDatum == null)
        assertTrue(txOuts[1].scriptRef == null)
        assertTrue(txOuts[2].address.equals("addr1wxr2a8htmzuhj39y2gq7ftkpxv98y2g67tg8zezthgq4jkg0a4ul4"))
        assertTrue(txOuts[2].amounts.filter { it.unit == "lovelace" }.map { it.quantity }.first().equals(BigInteger.valueOf(404000000L)))
        assertTrue(txOuts[2].amounts.size.equals(1))
        assertTrue(txOuts[2].datumHash.equals("f5ced47ca7d500234493a39650188f0ebe89e79554a2154f98805399e4704d90"))
        assertTrue(txOuts[2].inlineDatum == null)
        assertTrue(txOuts[2].scriptRef == null)
    }

    @Test
    fun getInputUtxos_2() {
        val txIns = setOf(
            TransactionInput("1973d456714f3fc83fa02c6f3f9640290381ce82edbabd085ca06ca936ca64d7", 1)
        )
        val txOuts = runBlocking { blockfrostService.getInputUtxos(txIns) }
        println("Retrieved txIn details, #: ${txOuts.size}, $txOuts")
        assertTrue(txOuts.isNotEmpty())
        assertTrue(txOuts[0].address.equals("addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a"))
        assertTrue(txOuts[0].amounts.filter { it.unit == "lovelace" }.map { it.quantity }.first().equals(BigInteger.valueOf(13723641057619L)))
        assertTrue(txOuts[0].datumHash == "102ae3ed1483f3e98b3a18bfaf5367dc4e4e8de6a991e0079ed2fbe41002b5b4")
        assertTrue(txOuts[0].inlineDatum == "d8799fd8799fd87a9f581c1eae96baf29e27682ea3f815aba361a0c6059d45e4bfbe95bbd2f44affffd8799f4040ffd8799f581c29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6434d494eff1b00003ab15584f9871b00000c7b47c0262e1b0001195186ac7f08181e1864d8799f190d05ffd87980ff")
        assertTrue(txOuts[0].scriptRef == null)
        assertTrue(txOuts[0].amounts.size.equals(4))
    }

    @Test
    fun getInputUtxos_3() {
        val txIns = setOf(
            TransactionInput("1973d456714f3fc83fa02c6f3f9640290381ce82edbabd085ca06ca936ca64d7", 10)
        )
        val txOuts = blockfrostService.getInputUtxos(txIns)
        println("Retrieved txIn details, #: ${txOuts.size}, $txOuts")
        assertTrue(txOuts.isEmpty())
    }

    @Test
    fun getBlockNearestToSlot() {
        val slot = 49400103L
        println("Getting block nearest to slot: $slot")
        val block = blockfrostService.getBlockNearestToSlot(slot)
        println("Block nearest to slot: $slot: $block")
        assertTrue(block?.hash.equals("af03c8d7813b2a68b5b0573fa598bb7c2838642790fcf679269426960e6d1487"))
        assertTrue(block?.height!!.equals(6698516L))
        assertTrue(block.slot.equals(49400102L))
        assertTrue(block.epoch.equals(311))
    }
}