package tech.edgx.prise.webserver.validator

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.validation.Errors
import org.springframework.validation.Validator
import tech.edgx.prise.webserver.model.prices.HistoricalCandlesRequest
import java.util.*
import java.util.regex.Pattern

@Component("getHistoricalPricesApiValidator")
class GetHistoricalPricesValidator : Validator {
    var dexLaunchDate: Date = GregorianCalendar(2022, Calendar.JANUARY, 1).time
    val unit_pattern = Pattern.compile("^\\p{XDigit}{56,500}(?::(?:ADA|\\p{XDigit}{56,500}))?$")

    override fun supports(clazz: Class<*>): Boolean {
        return HistoricalCandlesRequest::class.java == clazz
    }

    override fun validate(target: Any, errors: Errors) {
        val form: HistoricalCandlesRequest = target as HistoricalCandlesRequest
        if (form.symbol.isEmpty()) errors.rejectValue("symbol", "pair.required")
        if (form.resolution.isEmpty()) errors.rejectValue("resolution", "resolution.required")
        if (!errors.hasErrors()) {
            val unit_matcher = unit_pattern.matcher(form.symbol)
            try {
                if (!unit_matcher.matches()) {
                    errors.rejectValue("symbol", "symbol.invalid")
                }
            } catch (e: Exception) {
                errors.rejectValue("symbol", "symbol.invalid")
            }
            if (!allowedResolutions.contains(form.resolution)) {
                errors.rejectValue("resolution", "resolution.invalid")
            }
        }
        if (!errors.hasErrors()) {
            if (form.from != null) {
                log.debug("From date: ${form.from!! * 1000}, start: ${dexLaunchDate.time}, Less than start date: ${(form.from!! * 1000 < dexLaunchDate.time)}")
                if ((form.from!! * 1000 < dexLaunchDate.time || form.from!! > System.currentTimeMillis())) {
                    errors.rejectValue("from", "from.invalid")
                }
            }
            if (form.to != null) {
                log.debug("To: {}, launch date: {}, time now: {}", form.to, dexLaunchDate.time, System.currentTimeMillis())

                // Define an upper bound (e.g., 50 years from now in seconds)
                val maxAllowedTime = System.currentTimeMillis() / 1000 + 1_576_800_000 // 50 years in seconds

                if (form.to!! * 1000 < dexLaunchDate.time || form.to!! > maxAllowedTime) {
                    errors.rejectValue("to", "to.invalid")
                }
            }
        }
    }

    companion object {
        protected val log = LoggerFactory.getLogger(GetHistoricalPricesValidator::class.java)
        val allowedResolutions = listOf("1W", "1D", "1h", "15m")
    }
}