package tech.edgx.prise.indexer.service.dataprovider.module.blockfrost

import com.bloxbean.cardano.yaci.core.model.TransactionInput
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import org.koin.core.component.KoinComponent
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.BlockView
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/* NOT FULLY IMPLEMENTED */
class BlockfrostService(private val config: Config) : KoinComponent, ChainDatabaseService {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun getBlockNearestToSlot(slot: Long): BlockView? {
        // from provided time, calc abs slot, then count down 1 slot at a time until nearest block is found
        var block: BlockView?
        var slot = slot + 1
        runBlocking {
            do {
                val client = HttpClient.newBuilder().build();
                val request = buildGetRequest("/blocks/slot/${--slot}")
                log.debug("Blockfrost request: $request, headers: ${request.headers()}")
                val response = client.send(request, HttpResponse.BodyHandlers.ofString());
                block = Gson().fromJson(response.body(), BlockView::class.java)
            } while (block?.hash == null)
        }
        return block
    }

    override fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput> {
        log.warn("Not implemented")
        return emptyList()
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