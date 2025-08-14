package tech.edgx.prise.webserver.validator

import org.junit.jupiter.api.Test
import org.springframework.validation.MapBindingResult
import tech.edgx.prise.webserver.model.prices.LatestPricesRequest
import kotlin.test.assertTrue

class GetLatestPricesValidatorTest {

    val validator = GetLatestPricesValidator()

    @Test
    fun testLatestPrices_SingleAsset() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = LatestPricesRequest(symbols = setOf("ADA"))
        validator.validate(request, errors)
        assertTrue(!errors.hasErrors())
    }

    @Test
    fun testLatestPrices_NoAsset() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = LatestPricesRequest(symbols = setOf(""))
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("symbols"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("symbol.invalid"))
    }

    @Test
    fun testLatestPrices_HexAsset() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = LatestPricesRequest(symbols = setOf("533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459"))
        validator.validate(request, errors)
        assertTrue(!errors.hasErrors())
    }

    @Test
    fun testLatestPrices_MultipleAssets() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = LatestPricesRequest(symbols = setOf("533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459","279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b"))
        validator.validate(request, errors)
        assertTrue(!errors.hasErrors())
    }

    @Test
    fun testLatestPrices_InvalidHexChar() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = LatestPricesRequest(symbols = setOf("533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445H"))
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("symbols"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("symbol.invalid"))
    }
}