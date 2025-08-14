package tech.edgx.prise.indexer.service.dataprovider.module.blockfrost

import com.bloxbean.cardano.yaci.core.model.Amount
import com.bloxbean.cardano.yaci.core.model.TransactionInput
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.google.gson.Gson
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.koin.core.component.KoinComponent
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.BlockView
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.util.ExternalProviderException
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.TimeUnit

class BlockfrostService(private val config: Config) : KoinComponent, ChainDatabaseService {
    private val log = LoggerFactory.getLogger(javaClass)

    val MAX_BLOCK_BY_SLOT_ATTEMPTS = 250
    val MAX_UTXO_ATTEMPTS = 100
    val MAX_RETRIES_PER_REQUEST = 5
    val RETRY_DELAY_MS = 1000L
    val client = HttpClient.newBuilder().build()

    data class OutputDTO(val txHash: String?, val output: Outputs)

    /* Count down until a valid block is found */
    override fun getBlockNearestToSlot(slot: Long): BlockView? {
        var block: BlockView? = null
        var slotRequest = slot + 1
        var attempts = 0

        runBlocking {
            while (attempts < MAX_BLOCK_BY_SLOT_ATTEMPTS && block?.hash == null) {
                var retries = 0
                while (retries < MAX_RETRIES_PER_REQUEST) {
                    try {
                        val request = buildGetRequest("/blocks/slot/${--slotRequest}")
                        log.debug("Blockfrost request: $request, headers: ${request.headers()}")
                        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
                        block = Gson().fromJson(response.body(), BlockView::class.java)
                        break
                    } catch (e: IOException) {
                        retries++
                        if (retries >= MAX_RETRIES_PER_REQUEST) {
                            log.warn("Exhausted $MAX_RETRIES_PER_REQUEST retries for slot $slotRequest: ${e.message}")
                            break
                        }
                        val delayMs = RETRY_DELAY_MS * retries
                        log.info("Retry $retries/$MAX_RETRIES_PER_REQUEST for slot $slotRequest after IOException: ${e.message}, waiting ${delayMs}ms")
                        delay(delayMs)
                    } catch (e: Exception) {
                        log.error("Unexpected error for slot $slotRequest: ${e.message}")
                        throw ExternalProviderException("Unexpected failure at slot $slotRequest, error: ${e.message}");
                    }
                }
                attempts++
                if (block?.hash == null && attempts < MAX_BLOCK_BY_SLOT_ATTEMPTS) {
                    delay(RETRY_DELAY_MS)
                }
            }
        }

        if (block?.hash == null) {
            log.warn("No block found after $MAX_BLOCK_BY_SLOT_ATTEMPTS attempts from slot $slot")
            throw ExternalProviderException("Failed to get block by slot after $MAX_BLOCK_BY_SLOT_ATTEMPTS attempts")
        }
        return block
    }

    override fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput> {
        val txRequests = txIns.map {
            buildGetRequest("/txs/${it.transactionId}/utxos")
        }
        // BF doesn't provide endpoint to resolve specific tx output, resolve each whole tx first,
        val txs = txRequests.map { request ->
            log.debug("Blockfrost request: $request, headers: ${request.headers()}, body: $request")
            var attempts = 0
            var response: HttpResponse<String>
            /* May be unsynchronised or congested, try for MAX_ATTEMPTS then shutdown for rectification */
            runBlocking {
                do {
                    response = client.send(request, HttpResponse.BodyHandlers.ofString())
                    if (response.statusCode() != 200) {
                        log.debug("Blockfrost had error requesting utxos: $txIns")
                        if (attempts==0 || attempts % 100 == 0) log.info("Blockfrost had error requesting utxos... waiting ... attempts: $attempts")
                        if (attempts >= MAX_UTXO_ATTEMPTS) throw ExternalProviderException("Tried to get utxos from Blockfrost ${MAX_UTXO_ATTEMPTS} times and failed, exiting")
                        attempts++
                        delay(TimeUnit.SECONDS.toMillis(5))
                    }
                } while (response.statusCode() != 200)
            }
            val utxos: Transaction = Gson().fromJson(response.body(), Transaction::class.java)
            utxos
        }
        val seeking = txIns.map { it.transactionId+it.index }
        val txOutputs = txs.flatMap { tx ->
            val outputs = tx.outputs
                .filter { seeking.contains(tx.hash+it.outputIndex) }
                .map{ OutputDTO(tx.hash, it) }
            outputs
        }
        val txInRefMap = txOutputs.associateBy { it.txHash+it.output.outputIndex }
        val sameOrderedUtxos = txIns.mapNotNull { txInRefMap[it.transactionId+it.index] }
        val result = sameOrderedUtxos
            .map {
                val amounts: List<Amount> = it.output.amount
                    .map { asset ->
                        val (policy, name) = when(asset.unit=="lovelace") {
                            true -> Pair("lovelace","ada")
                            false -> Pair(asset.unit?.substring(0,56),asset.unit?.substring(56))
                        }
                        Amount(asset.unit, policy, name, null, asset.quantity?.toBigInteger())
                     }
                TransactionOutput(
                    it.output.address,
                    amounts,
                    it.output.dataHash,
                    it.output.inlineDatum,
                    it.output.referenceScriptHash) }
        return result
    }

    fun buildPostRequest(requestBody: String, snippet: String): HttpRequest {
        val requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create("${config.blockfrostDatasourceUrl}$snippet"))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
        if (!config.blockfrostDatasourceApiKey.isNullOrEmpty()) {
            requestBuilder.setHeader("project_id", config.blockfrostDatasourceApiKey)
        }
        return requestBuilder.build()
    }

    fun buildGetRequest(snippet: String): HttpRequest {
        val requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create("${config.blockfrostDatasourceUrl}$snippet"))
            .GET()
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
        if (!config.blockfrostDatasourceApiKey.isNullOrEmpty()) {
            requestBuilder.setHeader("project_id", config.blockfrostDatasourceApiKey)
        }
        return requestBuilder.build()
    }
}