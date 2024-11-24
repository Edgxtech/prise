package tech.edgx.prise.webserver.validator

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.validation.MapBindingResult
import tech.edgx.prise.webserver.model.prices.PriceHistoryRequest

class GetHistoricalPricesValidatorTest {

    val validator = GetHistoricalPricesValidator()

    @Test
    fun testHistoricalPrices_SingleAsset() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459", resolution = "1W", from=null, to=null)
        validator.validate(request, errors)
        assertTrue(!errors.hasErrors())
    }

    @Test
    fun testLatestPrices_NoAsset() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "", resolution = "1W", from=null, to=null)
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("symbol"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("symbol.required"))
    }

    @Test
    fun testLatestPrices_HexAsset() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459", resolution = "1W", from=null, to=null)
        validator.validate(request, errors)
        assertTrue(!errors.hasErrors())
    }

    @Test
    fun testLatestPrices_InvalidHexChar() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445H", resolution = "1W", from=null, to=null)
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("symbol"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("symbol.invalid"))
    }

    @Test
    fun testLatestPrices_NoResolution() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445b", resolution = "", from=null, to=null)
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("resolution"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("resolution.required"))
    }

    @Test
    fun testLatestPrices_UnknownResolution() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445b", resolution = "1y", from=null, to=null)
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("resolution"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("resolution.invalid"))
    }

    @Test
    fun testLatestPrices_ValidFrom() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445b", resolution = "1D", from=1722470400, to=null)
        validator.validate(request, errors)
        assertTrue(!errors.hasErrors())
    }

    @Test
    fun testLatestPrices_FromToEarly() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445b", resolution = "1D", from=722470400, to=null)
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("from"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("from.invalid"))
    }

    @Test
    fun testLatestPrices_ToOk() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445b", resolution = "1D", from=1722470400, to=1732243244)
        validator.validate(request, errors)
        assertTrue(!errors.hasErrors())
    }

    @Test
    fun testLatestPrices_ToToLate() {
        val errors = MapBindingResult(mapOf<Any,Any>(), "")
        val request = PriceHistoryRequest(symbol = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e445b", resolution = "1D", from=1722470400, to=11732243244)
        validator.validate(request, errors)
        assertTrue(errors.hasErrors())
        assertTrue(errors.hasFieldErrors("to"))
        assertTrue(errors.fieldErrors.map { it.code }.contains("to.invalid"))
    }
}