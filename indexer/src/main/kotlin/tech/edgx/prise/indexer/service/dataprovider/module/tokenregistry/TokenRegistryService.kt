package tech.edgx.prise.indexer.service.dataprovider.module.tokenregistry

import com.google.gson.Gson
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.model.dataprovider.BulkDecimals
import tech.edgx.prise.indexer.model.dataprovider.SubjectDecimalPair
import tech.edgx.prise.indexer.service.dataprovider.TokenMetadataService
import tech.edgx.prise.indexer.util.ExternalProviderException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class TokenRegistryService: TokenMetadataService {
    private val log = LoggerFactory.getLogger(javaClass)

    var MAX_ATTEMTPS = 2

    override fun getDecimals(units: List<String>): List<SubjectDecimalPair> {
        val values = mapOf("subjects" to units, "properties" to listOf("subject","decimals"))
        val requestBody = Gson().toJson(values)
        log.trace("Req body $requestBody")
        val client = HttpClient.newBuilder().build();
        val request = buildPostRequest(requestBody)
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var attempts = 1
        while (response.statusCode() == 429) {
            if (attempts > MAX_ATTEMTPS) {
                throw ExternalProviderException("Too many requests to token registry")
            }
            log.warn("TOO MANY REQUESTS, trying again, after sleeping, attempts $attempts")
            Thread.sleep((5 * 60000).toLong())
            response = client.send(request, HttpResponse.BodyHandlers.ofString())
            attempts++
        }
        /* Known to return 204 if none of the requested tokens have data */
        if (response.statusCode() != 200) {
            if (response.statusCode() != 204) log.warn("Couldn't get token registry info, status: ${response.statusCode()}, message: $response")
            return listOf()
        }
        log.trace("Token registry response status: ${response.statusCode()}")
        return Gson().fromJson(response.body(), BulkDecimals::class.java).subjects
    }

    fun buildPostRequest(requestBody: String): HttpRequest {
        return HttpRequest.newBuilder()
            .uri(URI.create("https://tokens.cardano.org/metadata/query"))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
            .build()
    }
}
