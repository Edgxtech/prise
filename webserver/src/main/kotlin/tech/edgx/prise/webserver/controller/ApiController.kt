package tech.edgx.prise.webserver.controller

import com.fasterxml.jackson.annotation.JsonInclude
import io.swagger.v3.oas.annotations.Hidden
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.validation.BindingResult
import org.springframework.web.bind.annotation.*
import tech.edgx.prise.webserver.model.prices.*
import tech.edgx.prise.webserver.service.price.PriceService
import tech.edgx.prise.webserver.util.InvalidRequestException
import tech.edgx.prise.webserver.validator.GetHistoricalPricesValidator
import tech.edgx.prise.webserver.validator.GetLatestPricesValidator
import jakarta.annotation.Resource
import jakarta.validation.Valid
import org.slf4j.LoggerFactory
import org.springframework.validation.FieldError
import tech.edgx.prise.webserver.model.tokens.AssetResponse
import tech.edgx.prise.webserver.model.tokens.TopByVolumeRequest
import tech.edgx.prise.webserver.model.tokens.TopByVolumeResponse
import tech.edgx.prise.webserver.service.AssetService
import tech.edgx.prise.webserver.validator.GetTopByVolumeValidator
import java.time.LocalDateTime

@Controller("ApiController")
@Tag(name = "Prise Data API", description = "Cardano Price API")
@RequestMapping(value = ["/"])
class ApiController {

    @Resource(name = "getPricesApiValidator")
    lateinit var getLatestPricesValidator: GetLatestPricesValidator

    @Resource(name = "getHistoricalPricesApiValidator")
    lateinit var getHistoricalPricesValidator: GetHistoricalPricesValidator

    @Resource(name = "getTopByVolumeValidator")
    lateinit var getTopByVolumeValidator: GetTopByVolumeValidator

    @Resource(name = "priceService")
    lateinit var priceService: PriceService

    @Resource(name = "assetService")
    lateinit var assetService: AssetService

    @Hidden
    @RequestMapping(value = ["/tokens/symbols"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    fun getDistinctSymbols(): ResponseEntity<Set<String>> {
        return ResponseEntity.ok(priceService.getDistinctAssets())
    }

    @Hidden
    @RequestMapping(value = ["/tokens/pairs"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    fun getDistinctPairs(): ResponseEntity<Set<Pair<String,String>>> {
        return ResponseEntity.ok(priceService.getAllSupportedPairs())
    }

    @RequestMapping(value = ["/tokens/top-by-volume"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    @Throws(Exception::class)
    fun getTopTokensByVolume(@Valid @ModelAttribute topByVolumeRequest: TopByVolumeRequest, errors: BindingResult): ResponseEntity<TopByVolumeResponse?> {
        getTopByVolumeValidator.validate(topByVolumeRequest, errors)
        log.debug("Form: {}, Has errors: {}", topByVolumeRequest, errors.hasErrors())
        if (!errors.hasErrors()) {
            val assets: Set<AssetResponse> = assetService.getTopByVolume(topByVolumeRequest)
            val response = TopByVolumeResponse(date = LocalDateTime.now(), assets = assets)
            log.debug("Returning Assets data, #: {}", assets.size)
            return ResponseEntity.ok(response)
        } else {
            throw InvalidRequestException(errors)
        }
    }

    @RequestMapping(value = ["/prices/latest"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    @Throws(Exception::class)
    fun getLatestPrices(@Valid @ModelAttribute latestPricesRequest: LatestPricesRequest, getPricesErrors: BindingResult): ResponseEntity<LatestPricesResponse?> {
        getLatestPricesValidator.validate(latestPricesRequest, getPricesErrors)
        log.debug("Form: {}, Has errors: {}", latestPricesRequest, getPricesErrors.hasErrors())
        if (!getPricesErrors.hasErrors()) {
            val assetPrices: List<AssetPrice> = priceService.getLatestPrices(latestPricesRequest)
            log.debug("Asset Prices #: {}", assetPrices.size)
            val response = LatestPricesResponse(date = LocalDateTime.now(), assets = assetPrices)
            log.debug("Returning Prices data, #: ${assetPrices.size}")
            return ResponseEntity.ok(response)
        } else {
            throw InvalidRequestException(getPricesErrors)
        }
    }

    @RequestMapping(value = ["/prices/historical/candles"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseBody
    @Throws(Exception::class)
    fun getHistoricalPrices(@ModelAttribute @Valid historicalCandlesRequest: HistoricalCandlesRequest?, errors: BindingResult): ResponseEntity<List<CandleResponse>> {
        // Handle invalid form binding when a required param is missing
        if (historicalCandlesRequest == null) {
            errors.addError(FieldError("historicalCandlesRequest", "symbol", "Symbol is required"))
            throw InvalidRequestException(errors)
        }
        getHistoricalPricesValidator.validate(historicalCandlesRequest, errors)
        log.debug("Form: {}, Has errors: {}", historicalCandlesRequest, errors.hasErrors())
        return if (!errors.hasErrors()) {
            log.debug("Fetching candles")
            val candles = priceService.getCandles(historicalCandlesRequest)
            log.debug("Returning # candles: {}", candles.size)
            ResponseEntity.ok(candles)
        } else {
            throw InvalidRequestException(errors)
        }
    }

    @Hidden
    @ExceptionHandler(IllegalArgumentException::class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ResponseBody
    fun handleIllegalArgument(e: IllegalArgumentException): ErrorResponse {
        log.error("Illegal argument error: {}", e.message)
        return ErrorResponse(
            errorCode = "NOT_FOUND",
            message = "The requested resource was not found."
        )
    }

    @Hidden
    @ExceptionHandler(InvalidRequestException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    fun handleInvalidRequest(e: InvalidRequestException): ErrorResponse {
        log.error("Invalid request: {}, {}, {}", e.message, e.errors, e)
        val errorMessages = e.errors.allErrors
            .map { it.defaultMessage ?: "Invalid input" }
            .filterNot { it.contains("Parameter specified as non-null is null") } // Filter out NPE messages
        return ErrorResponse(
            errorCode = "BAD_REQUEST",
            message = "Invalid request parameters.",
            details = errorMessages
        )
    }

    @Hidden
    @ExceptionHandler(Exception::class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    fun handleGenericException(e: Exception): ErrorResponse {
        log.error("Unexpected error occurred: ${e.message}", e)
        return ErrorResponse(
            errorCode = "INTERNAL_SERVER_ERROR",
            message = "An unexpected error occurred. Please try again later."
        )
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class ErrorResponse(
        val errorCode: String,
        val message: String,
        val details: List<String>? = null,
        val timestamp: LocalDateTime = LocalDateTime.now()
    )

    companion object {
        protected val log = LoggerFactory.getLogger(ApiController::class.java)

    }
}