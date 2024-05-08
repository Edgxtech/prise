package tech.edgx.prise.webserver.validator

import org.apache.commons.logging.LogFactory
import org.springframework.stereotype.Component
import org.springframework.validation.Errors
import org.springframework.validation.ValidationUtils
import org.springframework.validation.Validator
import tech.edgx.prise.webserver.model.prices.PriceHistoryRequest
import java.util.*
import java.util.regex.Pattern

@Component("getHistoricalPricesApiValidator")
class GetHistoricalPricesValidator : Validator {
    var dexLaunchDate: Date = GregorianCalendar(2022, Calendar.JANUARY, 1).time
    override fun supports(clazz: Class<*>): Boolean {
        return PriceHistoryRequest::class.java == clazz
    }

    override fun validate(target: Any, errors: Errors) {
        val form: PriceHistoryRequest = target as PriceHistoryRequest
        ValidationUtils.rejectIfEmptyOrWhitespace(errors, "symbol", "symbol.required")
        ValidationUtils.rejectIfEmptyOrWhitespace(errors, "resolution", "resolution.required")
        if (!errors.hasErrors()) {
            // Simple pattern check, not ideal for prod: symbol should be a unit. >56 chars and hex
            val unit_pattern = Pattern.compile("^\\p{XDigit}{56,500}+$")
            val unit_matcher = unit_pattern.matcher(form.symbol)
            try {
                if (!unit_matcher.matches() && !form.symbol.equals("lovelace")) {
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
                log.debug("To: ${form.to!!}, launch date: ${dexLaunchDate.time}, ${System.currentTimeMillis()}")
                if (form.to!! * 1000 < dexLaunchDate.time || form.to!! > System.currentTimeMillis() / 1000) {
                    errors.rejectValue("to", "to.invalid")
                }
            }
        }
    }

    companion object {
        protected val log = LogFactory.getLog(
            GetHistoricalPricesValidator::class.java
        )
        val allowedResolutions = listOf("1W", "1D", "1h", "15m")
    }
}