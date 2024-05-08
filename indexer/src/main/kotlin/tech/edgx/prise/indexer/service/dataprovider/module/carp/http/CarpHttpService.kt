package tech.edgx.prise.indexer.service.dataprovider.module.carp.http

import com.bloxbean.cardano.client.common.cbor.CborSerializationUtil
import com.bloxbean.cardano.yaci.core.model.Amount
import com.bloxbean.cardano.yaci.core.model.TransactionInput
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.bloxbean.cardano.yaci.core.util.HexUtil
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.koin.core.component.KoinComponent
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.domain.BlockView
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.service.dataprovider.module.carp.http.model.Pointer
import tech.edgx.prise.indexer.service.dataprovider.module.carp.http.model.UtxoAndBlockInfo
import tech.edgx.prise.indexer.service.dataprovider.module.carp.http.model.TransactionOutputRequest
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/* UNTESTED */
class CarpHttpService(private val config: Config) : KoinComponent, ChainDatabaseService {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun getBlockNearestToSlot(slot: Long): BlockView? {
        log.warn("Not implemented")
        return null
    }

    override fun getInputUtxos(txIns: Set<TransactionInput>): List<TransactionOutput> {
        val pointers = txIns.map { Pointer(it.transactionId, it.index) }
        val transactionOutputRequest = TransactionOutputRequest(pointers)
        val requestBody = Gson().toJson(transactionOutputRequest)
        log.trace("Req body $requestBody")
        val client = HttpClient.newBuilder().build();
        val request = buildPostRequest(requestBody)
        val response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            log.warn("Couldn't get inputUTXOs, status: ${response.statusCode()}, message: ${response}")
            return listOf()
        }
        val utxos: List<UtxoAndBlockInfo> = Gson().fromJson(response.body(), object : TypeToken<List<UtxoAndBlockInfo?>?>() {}.getType())
        return utxos.map { com.bloxbean.cardano.client.transaction.spec.TransactionOutput.deserialize(
            CborSerializationUtil.deserialize(HexUtil.decodeHexString(it.utxo.payload))) }
            .map {
                val amounts: MutableList<Amount> = it.value.multiAssets
                    .flatMap { ma -> ma.assets.map { Amount(ma.policyId+it.nameAsHex, ma.policyId, it.name, null, it.value) } }
                    .toMutableList()
                amounts.add(Amount("lovelace", "lovelace", "ada", null, it.value.coin))
                TransactionOutput(
                    it.address,
                    amounts,
                    com.bloxbean.cardano.client.util.HexUtil.encodeHexString(it.datumHash),
                    it.inlineDatum.datumHash,
                    com.bloxbean.cardano.client.util.HexUtil.encodeHexString(it.scriptRef)) }
    }

    fun buildPostRequest(requestBody: String): HttpRequest {
        val request = HttpRequest.newBuilder()
            .uri(URI.create(config.carpDatasourceUrl))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .setHeader("Content-type", "application/json;charset=utf-8")
            .build()
        return request
    }
}
