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
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.TimeUnit

class YaciStoreService(private val config: Config) : KoinComponent, ChainDatabaseService {
    private val log = LoggerFactory.getLogger(javaClass)

    val blockfrostService: ChainDatabaseService by inject(named("blockfrost")) { parametersOf(config) }

    val client = HttpClient.newBuilder().build()
    val MAX_ATTEMPTS = 500

    override fun getBlockNearestToSlot(slot: Long): BlockView? {
        // Temporarily delegating to blockfrost until better solution is available
        return blockfrostService.getBlockNearestToSlot(slot)
    }

    override fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput> {
        val transactionOutputRequest = txIns.map { TransactionOutputRequest(it.transactionId, it.index) }
        val requestBody = Gson().toJson(transactionOutputRequest)
        val request = buildPostRequest(requestBody, "/api/v1/utxos")
        log.debug("Yacistore request: $request, headers: ${request.headers()}, body: $requestBody")
        var attempts = 0
        var response: HttpResponse<String>
        /* May be unsynchronised or congested, try for MAX_ATTEMPTS then shutdown for rectification */
        runBlocking {
            do {
                response = client.send(request, HttpResponse.BodyHandlers.ofString())
                log.debug("Response status: ${response.statusCode()}")
                if (!(response.statusCode() == 200 || response.statusCode()==404)) {
                    log.debug("Yacistore had error requesting utxos: $txIns")
                    if (attempts==0 || attempts % 100 == 0) log.info("Yacistore had error requesting utxos... waiting ... attempts: $attempts")
                    if (attempts >= MAX_ATTEMPTS) throw ExternalProviderException("Tried to get utxos from Yacistore ${MAX_ATTEMPTS} times and failed, exiting")
                    attempts++
                    delay(TimeUnit.SECONDS.toMillis(5))
                }
            } while (!(response.statusCode() == 200 || response.statusCode() ==404))
        }
        when (response.statusCode()==404) {
            true -> return emptyList()
            false -> {
                val utxos: List<UtxoDetails> = Gson().fromJson(response.body(), object : TypeToken<List<UtxoDetails>>() {}.type)
                val txInRefMap = utxos.associateBy { it.txHash+it.outputIndex }
                val sameOrderedUtxos = txIns.mapNotNull { txInRefMap[it.transactionId+it.index] }
                return sameOrderedUtxos
                    .map {
                        val amounts: List<Amount> = it.amounts
                            .map { asset ->
                                val (policy, name) = when(asset.unit=="lovelace") {
                                    true -> Pair("lovelace","ada")
                                    false -> Pair(asset.unit?.substring(0,56),asset.unit?.substring(56))
                                }
                                Amount(asset.unit, policy, name, null, asset.quantity?.toBigInteger())
                             }
                        TransactionOutput(
                            it.ownerAddr,
                            amounts,
                            it.dataHash,
                            it.inlineDatum,
                            it.scriptRef)
                    }
            }
        }
    }

    fun buildPostRequest(requestBody: String, snippet: String): HttpRequest {
        val requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create("${config.yacistoreDatasourceUrl}$snippet"))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
        return requestBuilder.build()
    }

    fun buildGetRequest(snippet: String): HttpRequest {
        val requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create("${config.yacistoreDatasourceUrl}$snippet"))
            .GET()
            .setHeader("Accepts", "application/json")
        return requestBuilder.build()
    }
}