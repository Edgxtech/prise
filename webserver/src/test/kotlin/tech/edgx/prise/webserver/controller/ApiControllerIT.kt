package tech.edgx.prise.webserver.controller

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.http.HttpStatus
import tech.edgx.prise.webserver.model.prices.Candle
import tech.edgx.prise.webserver.model.prices.Close
import tech.edgx.prise.webserver.model.prices.LatestPricesResponse
import tech.edgx.prise.webserver.util.PricingProviderEnum
import tech.edgx.prise.webserver.util.TestHelpers
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.*
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiControllerIT {

    val client = HttpClient.newBuilder().build()
    val pricingProviderList = PricingProviderEnum.values().map { it.code }

    @BeforeAll
    fun populateTestData() {
        TestHelpers.populateDatabaseWithTestData()
    }

    private fun buildGetRequest(snippet: String): HttpRequest {
        return HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8092/api$snippet"))
            .GET()
            .setHeader("Accepts", "application/json")
            .setHeader("Content-type", "application/json;charset=utf-8")
            .build()
    }

    @Test
    fun getLatestPrices_All() {
        val request = buildGetRequest("/prices/latest")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_ = Gson().fromJson(response.body(), LatestPricesResponse::class.java)
        assertTrue(response_.assets.isNotEmpty())
        assertTrue(response_.assets.size==678)
        val snekList = response_.assets.filter { it.symbol=="279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b" }
        snekList.forEach {
            assertEquals("SNEK",it.name)
            assertTrue(pricingProviderList.contains(it.pricing_provider))
            when (it.pricing_provider) {
                PricingProviderEnum.MINSWAP.code -> {
                    assertEquals(0.0022971155855701295, it.last_price_ada)
                    assertEquals("Wed Aug 07 08:37:45 MYT 2024", it.last_update.toString())
                }
                PricingProviderEnum.MINSWAPV2.code -> {
                    assertEquals(0.002276563914791377, it.last_price_ada)
                    assertEquals("Wed Aug 07 07:54:57 MYT 2024", it.last_update.toString())
                }
                PricingProviderEnum.SUNDAESWAP.code -> {
                    assertEquals(0.0022824224815309402, it.last_price_ada)
                    assertEquals("Wed Aug 07 02:07:17 MYT 2024", it.last_update.toString())
                }
                PricingProviderEnum.WINGRIDERS.code -> {
                    assertEquals(0.0023037710318390536, it.last_price_ada)
                    assertEquals("Wed Aug 07 06:40:08 MYT 2024", it.last_update.toString())
                }
            }
        }
    }

    @Test
    fun getLatestPrices_Specific() {
        val request = buildGetRequest("/prices/latest?symbol=533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_ = Gson().fromJson(response.body(), LatestPricesResponse::class.java)
        assertTrue(response_.assets.isNotEmpty())
        response_.assets.forEach {
            assertEquals("INDY",it.name)
            assertTrue(pricingProviderList.contains(it.pricing_provider))
            when (it.pricing_provider) {
                PricingProviderEnum.MINSWAP.code -> {
                    assertEquals(1.5773350451778232, it.last_price_ada)
                    assertEquals("Wed Aug 07 08:46:01 MYT 2024", it.last_update.toString())
                }
                PricingProviderEnum.MINSWAPV2.code -> {
                    assertEquals(1.597229701426458, it.last_price_ada)
                    assertEquals("Wed Aug 07 08:47:11 MYT 2024", it.last_update.toString())
                }
                PricingProviderEnum.SUNDAESWAP.code -> {
                    assertEquals(1.5795027165611124, it.last_price_ada)
                    assertEquals("Wed Aug 07 08:46:01 MYT 2024", it.last_update.toString())
                }
                PricingProviderEnum.WINGRIDERS.code -> {
                    assertEquals(1.5772318929230171, it.last_price_ada)
                    assertEquals("Wed Aug 07 08:46:01 MYT 2024", it.last_update.toString())
                }
            }
        }
    }

    @Test
    fun getLatestPrices_SpecificNoToken() {
        val request = buildGetRequest("/prices/latest?symbol=533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_ = Gson().fromJson(response.body(), LatestPricesResponse::class.java)
        assertTrue(response_.assets.isEmpty())
    }

    @Test
    fun getLatestPrices_SpecificInvalidHex() {
        val request = buildGetRequest("/prices/latest?symbol=533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445H")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(HttpStatus.BAD_REQUEST.value(), response.statusCode())
    }

    @Test
    fun getHistoricalPrices_Weekly() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=1W")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isNotEmpty())
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.002303199123340428, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalClose_Weekly() {
        val request = buildGetRequest("/prices/historical/close-prices?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=1W")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Close> = Gson().fromJson(response.body(), object : TypeToken<List<Close>>() {}.type)
        assertTrue(response_.isNotEmpty())
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.002303199123340428, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalPrices_Daily() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=1D")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isNotEmpty())
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.0022973671039298784, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalPrices_Hourly() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=1h")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isNotEmpty())
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.002316074692253739, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalPrices_Fifteen() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=15m")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isNotEmpty())
        assertEquals(579,response_.size)
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.002315519596210169, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalPrices_Daily_Slice() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=1D&to=1722911400")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isNotEmpty())
        assertEquals(6,response_.size)
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.0022973671039298784, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalPrices_Hourly_Slice() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=1h&from=1722474900&to=1722911400")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isNotEmpty())
        assertEquals(121,response_.size)
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.0023152737410985806, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalPrices_Fifteen_Slice() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b&resolution=15m&from=1722474900&to=1722911400")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isNotEmpty())
        assertEquals(484,response_.size)
        val avgClose = response_.mapNotNull { it.close }.reduce {a,b -> a+b} / response_.size
        assertEquals(0.0023155756142586342, avgClose, 0.000000000001)
    }

    @Test
    fun getHistoricalPrices_NoToken() {
        val request = buildGetRequest("/prices/historical/candles?symbol=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454&resolution=1W")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<Candle> = Gson().fromJson(response.body(), object : TypeToken<List<Candle>>() {}.type)
        assertTrue(response_.isEmpty())
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
        val request = buildGetRequest("/symbols")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        val response_: List<String> = Gson().fromJson(response.body(), object : TypeToken<List<String>>() {}.type)
        assertEquals(461, response_.size)
    }

    @Test
    fun getNotFound() {
        val request = buildGetRequest("/prices/notanendpoint")
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(HttpStatus.NOT_FOUND.value(), response.statusCode())
    }
}