package tech.edgx.prise.webserver.validator

import org.apache.commons.logging.LogFactory
import org.springframework.stereotype.Component
import org.springframework.validation.Errors
import org.springframework.validation.Validator
import tech.edgx.prise.webserver.model.prices.LatestPricesRequest
import tech.edgx.prise.webserver.util.Helpers
import java.util.*
import java.util.regex.Pattern

@Component("getPricesApiValidator")
class GetLatestPricesValidator : Validator {
    var allowedNetworks = listOf(Helpers.MAINNET_ID, Helpers.TESTNET_ID)
    override fun supports(clazz: Class<*>): Boolean {
        return LatestPricesRequest::class.java == clazz
    }

    override fun validate(target: Any, errors: Errors) {
        val form: LatestPricesRequest = target as LatestPricesRequest
        if (!errors.hasErrors()) {
            if (!allowedNetworks.contains(form.network)) {
                errors.rejectValue("network", "network.invalid")
            }
            if (form.symbol.size > 500) {
                errors.rejectValue("symbol", "symbol.max")
            }
            for (asset_id in form.symbol) {
                // Simple pattern check, not ideal for prod: symbol should be a unit. >56 chars and hex
                val unit_pattern = Pattern.compile("^\\p{XDigit}{56,500}+$")
                val unit_matcher = unit_pattern.matcher(asset_id)
                try {
                    if (!unit_matcher.matches() && asset_id != Helpers.ADA_ASSET_UNIT) {
                        errors.rejectValue("symbol", "symbol.invalid")
                    }
                } catch (e: Exception) {
                    errors.rejectValue("symbol", "symbol.invalid")
                }
            }
        }
    }

    companion object {
        protected val log = LogFactory.getLog(
            GetLatestPricesValidator::class.java
        )
    }
}