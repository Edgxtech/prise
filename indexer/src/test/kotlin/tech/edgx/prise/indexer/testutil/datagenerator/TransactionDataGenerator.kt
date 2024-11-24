package tech.edgx.prise.indexer.testutil.datagenerator

import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.helper.reactive.BlockStreamer
import com.google.gson.Gson
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Ignore
import org.junit.jupiter.api.Test
import org.koin.core.parameter.parametersOf
import org.koin.test.inject
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.BaseWithCarp
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.repository.CarpRepository
import tech.edgx.prise.indexer.service.chain.ChainService
import java.io.File
import java.io.PrintWriter

class TransactionDataGenerator: BaseWithCarp() {
    private val log = LoggerFactory.getLogger(javaClass)
    val chainService: ChainService by inject { parametersOf(config) }
    val carpRepository: CarpRepository by inject { parametersOf(config.carpDatabase) }

    @Ignore
    @Test
    fun saveSpecificQualifiedTransactionToJson() {
        //val txHash = "fa30415a3d5e336b80d940d880461bcf19cd1c63b2981ccbe51c1f30557feb82"; val txSlot = 132239546L
        //val txHash = "d3aba39861706b25c5a8c33ce48889be998405a55b646002ee2f085e5a9fcd14"; val txSlot = 132222216L
        //val txHash = "bee485180e6ae05af621ac0e560fbfaf57b8563f81e95b960930129d12ecdd94"; val txSlot = 132476966L
        //val txHash = "e0b5b5e5d18878c9b502c0d39f9b813d6fc0f2ace3e2dbb98bd97ee19c298872"; val txSlot = 132459047L
        //val txHash = "592ac794f7ea60aba502b44aa436178626bef087ce7f05bf4a53dbd1456287be"; val txSlot = 132219880L
        //val txHash = "fa30415a3d5e336b80d940d880461bcf19cd1c63b2981ccbe51c1f30557feb82"; val txSlot = 132239546L
        //val txHash = "72875a21809e7c75d0e98e4751171eafc66847dc2614fa70208cfc067d565d90"; val txSlot = 116200902L
        //val txHash = "b02042417e8fa4e2386b0e47c85e3f2d18e3483196c861767758ccba52e75730"; val txSlot = 116978969L
        //val txHash = "3cae4bea2849f1cc8546a96058320f0deb949289cbd58295cfc7f50dab71f15a"; val txSlot = 112501470L
        val txHash = "24983065abb54ff66368fda2c32372325bac4a1320452fd7643210699e76c6ae"; val txSlot = 130934066L
        val dexName = DexEnum.MINSWAPV2.nativeName
        val thisBlock = carpRepository.getBlockNearestToSlot(txSlot)
        val previousBlock = thisBlock?.height?.minus( 1)?.let { carpRepository.getBlockByHeight(it) }
        println("This block: $thisBlock, Previous block: $previousBlock")
        val startPoint = previousBlock?.slot?.let { Point(it, previousBlock.hash) }

        val outputFile = "src/test/resources/testdata/$dexName/transaction_qualified_${txHash}.json"

        val writer: PrintWriter = File(outputFile).printWriter()
        var processedBlock = false
        val blockStreamer = BlockStreamer.fromPoint(config.cnodeAddress, config.cnodePort!!, startPoint,  NetworkType.MAINNET.n2NVersionTable)
        val blockFlux = blockStreamer.stream()
        val subscription = blockFlux.subscribe { block: Block ->
            log.info("Received Block >> ${block.header.headerBody.blockNumber}, ${block.header.headerBody.blockHash}, slot: ${block.header.headerBody.slot}, Txns # >> ${block.transactionBodies.size}")
            val qualifiedTx = chainService.qualifyTransactions(block.header.headerBody.slot, block.transactionBodies, block.transactionWitness)
            val filteredDexSpecificTx = qualifiedTx
                .firstOrNull { it.txHash == txHash }
            println("Found transactions for DEX #: ${filteredDexSpecificTx}")

            if (filteredDexSpecificTx != null) {
                writer.println(Gson().toJson(filteredDexSpecificTx))
                writer.flush()
                writer.close()

            }
            processedBlock = true
        }
        runBlocking {
            while(true) {
                if (processedBlock) {
                    break
                }
                delay(100)
            }
            subscription.dispose()
            blockStreamer.shutdown()
        }
    }
}