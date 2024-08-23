package tech.edgx.prise.indexer.service.dataprovider.module.koios

import com.bloxbean.cardano.yaci.core.model.Amount
import com.bloxbean.cardano.yaci.core.model.TransactionInput
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
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
class KoiosService(private val config: Config) : KoinComponent, ChainDatabaseService {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun getBlockNearestToSlot(slot: Long): BlockView? {
        log.warn("Not implemented")
        return null
    }

    override fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput> {
        val txInStringRefs = txIns.map { "${it.transactionId}#${it.index}" }
        val transactionOutputRequest = TransactionOutputRequest(txInStringRefs,true)
        val requestBody = Gson().toJson(transactionOutputRequest)
        val client = HttpClient.newBuilder().build();
        val request = buildPostRequest(requestBody, "/utxo_info")
        log.debug("Koios request: $request, headers: ${request.headers()}, body: $requestBody")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            log.warn("Couldn't get inputUTXOs, status: ${response.statusCode()}, message: ${response}")
            return listOf()
        }
        val utxos: List<UtxoDetails> = Gson().fromJson(response.body(), object : TypeToken<List<UtxoDetails>>() {}.type)
        val txInRefMap = utxos.associateBy { it.tx_hash+it.tx_index }
        val sameOrderedUtxos = txIns.map { txInRefMap[it.transactionId+it.index]!! }
        return sameOrderedUtxos
            .map {
                val amounts: MutableList<Amount> = it.asset_list
                    .map { asset -> Amount(asset.policy_id+asset.asset_name, asset.policy_id, asset.asset_name, null, asset.quantity.toBigInteger()) }
                    .toMutableList()
                amounts.add(Amount("lovelace", "lovelace", "ada", null, it.value.toBigInteger()))
                TransactionOutput(
                    it.address,
                    amounts,
                    it.datum_hash,
                    it.inline_datum,
                    it.reference_script) }
    }

    fun buildPostRequest(requestBody: String, snippet: String): HttpRequest {
        val requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create("${config.koiosDatasourceUrl}$snippet"))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
        if (!config.koiosDatasourceApiKey.isNullOrEmpty()) {
            requestBuilder.setHeader("Authorization", "Bearer ${config.koiosDatasourceApiKey}")
        }
        return requestBuilder.build()
    }
}