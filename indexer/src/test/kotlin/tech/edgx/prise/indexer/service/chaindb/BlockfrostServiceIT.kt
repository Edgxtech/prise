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