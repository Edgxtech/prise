package tech.edgx.prise.webserver.controller

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.http.HttpStatus
import tech.edgx.prise.webserver.model.prices.LatestPricesResponse
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiControllerIT {

    val client = HttpClient.newBuilder().build()

    val objectMapper = ObjectMapper()
        .registerModule(JavaTimeModule())
        .registerModule(KotlinModule.Builder().build())

    private fun buildGetRequest(snippet: String): HttpRequest {
        return HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8092$snippet"))
            .GET()
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
            .build()
    }

    @Test
    fun getLatestPrices_SpecificNoToken() {
        val request = buildGetRequest("/prices/latest?symbols=533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_ = objectMapper.readValue(response.body(), LatestPricesResponse::class.java)
        assertTrue(response_.assets.isEmpty())
    }

    @Test
    fun getLatestPrices_SpecificInvalidHex() {
        val request = buildGetRequest("/prices/latest?symbols=533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445H")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(HttpStatus.BAD_REQUEST.value(), response.statusCode())
    }

    @Test
    fun getHistoricalPrices_SymbolEmpty() {
        val request = buildGetRequest("/prices/historical/candles?symbol=&resolution=1W")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(HttpStatus.BAD_REQUEST.value(), response.statusCode())
    }

    @Test
    fun getHistoricalPrices_SymbolMissing() {
        val request = buildGetRequest("/prices/historical/candles?resolution=1W")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(HttpStatus.BAD_REQUEST.value(), response.statusCode())
    }

    @Test
    fun getHistoricalPrices_InvalidHex() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e45H&resolution=1W")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(HttpStatus.BAD_REQUEST.value(), response.statusCode())
    }

    @Test
    fun getSymbolsList() {
        val request = buildGetRequest("/tokens/symbols")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<String> = objectMapper.readValue(response.body(), object : TypeReference<List<String>>() {})
        assertEquals(HttpStatus.OK.value(), response.statusCode())
    }

    @Test
    fun getNotFound() {
        val request = buildGetRequest("/prices/notanendpoint")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(HttpStatus.NOT_FOUND.value(), response.statusCode())
    }
}