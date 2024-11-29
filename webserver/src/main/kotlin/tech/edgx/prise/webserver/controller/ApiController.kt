package tech.edgx.prise.webserver.controller

import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import org.apache.commons.logging.LogFactory
import org.junit.jupiter.api.DisplayName
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.validation.BindingResult
import org.springframework.validation.ObjectError
import org.springframework.web.bind.annotation.*
import tech.edgx.prise.webserver.domain.Candle
import tech.edgx.prise.webserver.domain.Close
import tech.edgx.prise.webserver.model.prices.AssetPrice
import tech.edgx.prise.webserver.model.prices.LatestPricesRequest
import tech.edgx.prise.webserver.model.prices.LatestPricesResponse
import tech.edgx.prise.webserver.model.prices.PriceHistoryRequest
import tech.edgx.prise.webserver.service.AssetService
import tech.edgx.prise.webserver.service.PriceService
import tech.edgx.prise.webserver.util.InvalidRequestException
import tech.edgx.prise.webserver.validator.GetHistoricalPricesValidator
import tech.edgx.prise.webserver.validator.GetLatestPricesValidator
import java.util.*
import java.util.stream.Collectors
import javax.annotation.Resource

@Controller("ApiController")
@DisplayName("Prise Data API")
@RequestMapping(value = ["/api"])
@CrossOrigin("http://localhost:49430")
class ApiController {

    @Resource(name="getPricesApiValidator")
    lateinit var getLatestPricesValidator: GetLatestPricesValidator

    @Resource(name="getHistoricalPricesApiValidator")
    lateinit var getHistoricalPricesValidator: GetHistoricalPricesValidator

    @Resource(name="assetService")
    lateinit var assetService: AssetService

    @Resource(name = "priceService")
    lateinit var priceService: PriceService

    @RequestMapping(value = ["/symbols"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    fun getDistinctSymbols(): ResponseEntity<Set<String>> {
        return ResponseEntity.ok(assetService.getDistinctSymbols())
    }

    @ApiResponses(value = [ApiResponse(responseCode = "400", description = "Bad request", content = [Content()])])
    @RequestMapping(value = ["/prices/latest"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiImplicitParams(
        ApiImplicitParam(name = "symbol", required = false, allowMultiple = true, paramType = "query", dataType = "array")
    )
    @ResponseBody
    @Throws(Exception::class)
    fun getLatestPrices(latestPricesRequest: LatestPricesRequest, getPricesErrors: BindingResult): ResponseEntity<LatestPricesResponse?>? {
        getLatestPricesValidator.validate(latestPricesRequest, getPricesErrors)
        log.debug("Form: $latestPricesRequest, Has errors: ${getPricesErrors.hasErrors()}")
        if (!getPricesErrors.hasErrors()) {
            val assetPrices: List<AssetPrice>? = assetService.getCNTPriceList(latestPricesRequest.symbol)
            val response = LatestPricesResponse(date=Date(), assets=assetPrices ?: listOf())
            log.debug("Returning Prices data, #: ${assetPrices?.size}")
            return ResponseEntity.ok<LatestPricesResponse>(response)
        } else {
            throw InvalidRequestException(getPricesErrors)
        }
    }

    /* Example: localhost:8092/api/prices/historical/candles?resolution=1D&symbol=533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459 */
    @ApiResponses(value = [ApiResponse(responseCode = "400", description = "Bad request", content = [Content()])])
    @RequestMapping(value = ["/prices/historical/candles"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    @Throws(Exception::class)
    fun getHistoricalPrices(priceHistoryRequest: PriceHistoryRequest, errors: BindingResult): ResponseEntity<List<Candle?>> {
        log.debug("Submitted form: $priceHistoryRequest")
        getHistoricalPricesValidator.validate(priceHistoryRequest, errors)
        return if (!errors.hasErrors()) {
            // NOTE: The order here is first == oldest, last == newest, compatible with TV charts
            val candles = priceService.getCandles(priceHistoryRequest)
            log.debug("Returning # candles: ${candles?.size}")
            ResponseEntity.ok(candles)
        } else {
            throw InvalidRequestException(errors)
        }
    }

    /* Example: localhost:8084/api/prices/historical/close-prices?resolution=1D&symbol=<unit> */
    @ApiResponses(value = [ApiResponse(responseCode = "400", description = "Bad request", content = [Content()])])
    @RequestMapping(value = ["/prices/historical/close-prices"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    @Throws(Exception::class)
    fun getHistoricalClosingPrices(priceHistoryRequest: PriceHistoryRequest, errors: BindingResult): ResponseEntity<List<Close?>> {
        log.debug("Submitted form: $priceHistoryRequest")
        getHistoricalPricesValidator.validate(priceHistoryRequest, errors);
        return if (!errors.hasErrors()) {
            val closes = priceService.getCloses(priceHistoryRequest)
            log.debug("Returning # prices: ${closes?.size}")
            ResponseEntity.ok(closes)
        } else {
            throw InvalidRequestException(errors)
        }
    }

    @ExceptionHandler(InvalidRequestException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    fun showCustomMessageInvalidInput(e: Exception): Map<String, String> {
        val response: MutableMap<String, String> = HashMap()
        val ire = e as InvalidRequestException
        response["status"] = "request invalid: " + ire.errors.allErrors.stream().map { er: ObjectError -> er.code }
            .collect(Collectors.toList())
        return response
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    fun showCustomMessage(e: Exception?): Map<String, String> {
        val response: MutableMap<String, String> = HashMap()
        response["status"] = "unknown error"
        log.error(e, e)
        return response
    }

    companion object {
        protected val log = LogFactory.getLog(ApiController::class.java)
    }
}