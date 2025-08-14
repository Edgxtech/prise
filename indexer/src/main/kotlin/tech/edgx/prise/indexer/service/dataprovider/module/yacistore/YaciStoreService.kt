package tech.edgx.prise.indexer.service.dataprovider.module.yacistore

import com.bloxbean.cardano.yaci.core.model.Amount
import com.bloxbean.cardano.yaci.core.model.TransactionInput
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.BlockView
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.util.ExternalProviderException
import java.lang.Exception
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration

class YaciStoreService(private val config: Config) : KoinComponent, ChainDatabaseService {
    private val log = LoggerFactory.getLogger(javaClass)

    val blockfrostService: ChainDatabaseService by inject(named("blockfrost")) { parametersOf(config) }

    val client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    val MAX_ATTEMPTS = 50

    override fun getBlockNearestToSlot(slot: Long): BlockView? {
        // Temporarily delegating to blockfrost until better solution is available
        return blockfrostService.getBlockNearestToSlot(slot)
    }

    override fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput> {
        val transactionOutputRequest = txIns.map { TransactionOutputRequest(it.transactionId, it.index) }
        val requestBody = Gson().toJson(transactionOutputRequest)
        val request = buildPostRequest(requestBody, "/api/v1/utxos", config.yacistoreDatasourceApiKey)
        log.debug("Yacistore request: {}, headers: {}, body: {}", request, request.headers(), requestBody)

        val maxAttempts = 100 // Set high since this indexer may be ahead of the other indexer
        var attempts = 0
        val baseDelay = 1000L // 1 second
        val maxDelay = 16000L // 16 seconds
        var currentDelay = baseDelay
        var response: HttpResponse<String>?

        /* May be unsynchronized or congested, try for maxAttempts then fallback */
        runBlocking {
            do {
                response = try {
                    client.send(request, HttpResponse.BodyHandlers.ofString())
                } catch (e: HttpTimeoutException) {
                    log.warn("Yacistore request timed out: ${e.message}")
                    null
                } catch (e: Exception) {
                    log.error("Yacistore request failed: ${e.message}")
                    null
                }
                log.debug("Yacistore request: $request, headers: ${request.headers()}, body: $requestBody, response status: ${response?.statusCode()}, response body: ${response?.body()}")
                if (response == null || !(response!!.statusCode() == 200 || response!!.statusCode() == 404)) {
                    val status = response?.statusCode() ?: "no response"
                    log.info("Yacistore had error requesting utxos: $txIns at $request, status: $status, body: ${response?.body()}")
                    if (status in listOf(400, 401, 403)) {
                        throw ExternalProviderException("Non-retryable error from Yacistore: status $status, body: ${response?.body()}")
                    }
                    if (attempts >= maxAttempts) {
                        log.warn("Yacistore failed after $maxAttempts attempts, falling back to Blockfrost")
                        //monitoringService.incrementCounter("yacistore_fallback")
                        //return@runBlocking blockfrostService.getInputUtxos(txIns)
                        throw ExternalProviderException("Yacistore Request failed after $maxAttempts attempts: status $status, body: ${response?.body()}")
                    }
                    attempts++
                    log.info("Retrying Yacistore request, attempt ${attempts + 1}/$maxAttempts, waiting ${currentDelay}ms")
                    delay(currentDelay)
                    currentDelay = minOf(currentDelay * 2, maxDelay)
                } else {
                    currentDelay = baseDelay // Reset delay on success
                }
            } while (response == null || !(response!!.statusCode() == 200 || response!!.statusCode() == 404))
        }

        return when (response?.statusCode() == 404) {
            true -> emptyList()
            false -> {
                val utxos: List<UtxoDetails> = Gson().fromJson(response?.body(), object : TypeToken<List<UtxoDetails>>() {}.type)
                val txInRefMap = utxos.associateBy { it.txHash + it.outputIndex }
                val sameOrderedUtxos = txIns.mapNotNull { txInRefMap[it.transactionId + it.index] }
                sameOrderedUtxos.map {
                    val amounts: List<Amount> = it.amounts
                        .map { asset ->
                            val (policy, name) = when (asset.unit == "lovelace") {
                                true -> Pair("lovelace", "ada")
                                false -> Pair(asset.unit?.substring(0, 56), asset.unit?.substring(56))
                            }
                            Amount(asset.unit, policy, name, null, asset.quantity?.toBigInteger())
                        }
                    TransactionOutput(
                        it.ownerAddr,
                        amounts,
                        it.dataHash,
                        it.inlineDatum,
                        it.scriptRef
                    )
                }
            }
        }
    }

    fun buildPostRequest(requestBody: String, snippet: String, apiKey: String?): HttpRequest {
        val requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create("${config.yacistoreDatasourceUrl}$snippet"))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
            .timeout(Duration.ofSeconds(30)) // Timeout for the entire request
        if (apiKey != null) {
            requestBuilder.setHeader("Authorization", "Bearer $apiKey")
        }
        return requestBuilder.build()
    }

    fun buildGetRequest(snippet: String): HttpRequest {
        val requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create("${config.yacistoreDatasourceUrl}$snippet"))
            .GET()
            .setHeader("Accepts", "application/json")
            .timeout(Duration.ofSeconds(30)) // Timeout for the entire reques
        return requestBuilder.build()
    }
}