package tech.edgx.prise.webserver.validator

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.validation.Errors
import org.springframework.validation.Validator
import tech.edgx.prise.webserver.model.prices.LatestPricesRequest
import tech.edgx.prise.webserver.util.Helpers
import java.util.regex.Pattern

@Component("getPricesApiValidator")
class GetLatestPricesValidator : Validator {
    var allowedNetworks = listOf(Helpers.MAINNET_ID, Helpers.TESTNET_ID)
    val unit_pattern = Pattern.compile("^\\p{XDigit}{56,500}(?::(?:ADA|\\p{XDigit}{56,500}))?$")
    override fun supports(clazz: Class<*>): Boolean {
        return LatestPricesRequest::class.java == clazz
    }

    override fun validate(target: Any, errors: Errors) {
        val form: LatestPricesRequest = target as LatestPricesRequest
        if (!errors.hasErrors()) {
            if (!allowedNetworks.contains(form.network)) {
                errors.rejectValue("network", "network.invalid")
            }
            if (form.symbols.size > 500) {
                errors.rejectValue("symbols", "symbol.max")
            }
            for (asset_id in form.symbols) {
                // Simple pattern check, not ideal for prod: symbol should be a unit. >56 chars and hex
                val unit_matcher = unit_pattern.matcher(asset_id)
                try {
                    if (!unit_matcher.matches() && asset_id != "ADA") {
                        errors.rejectValue("symbols", "symbol.invalid")
                    }
                } catch (e: Exception) {
                    errors.rejectValue("symbols", "symbol.invalid")
                }
            }
        }
    }

    companion object {
        protected val log = LoggerFactory.getLogger(
            GetTopByVolumeValidator::class.java
        )
    }
}