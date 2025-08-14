package tech.edgx.prise.webserver.validator

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.validation.Errors
import org.springframework.validation.Validator
import tech.edgx.prise.webserver.model.tokens.TopByVolumeRequest
import tech.edgx.prise.webserver.util.Helpers

@Component("getTopByVolumeValidator")
class GetTopByVolumeValidator : Validator {
    var allowedNetworks = listOf(Helpers.MAINNET_ID, Helpers.TESTNET_ID)
    override fun supports(clazz: Class<*>): Boolean {
        return TopByVolumeRequest::class.java == clazz
    }

    override fun validate(target: Any, errors: Errors) {
        val form: TopByVolumeRequest = target as TopByVolumeRequest
        if (!errors.hasErrors()) {
            if (!allowedNetworks.contains(form.network)) {
                errors.rejectValue("network", "network.invalid")
            }
            if (form.limit > 1000) {
                errors.rejectValue("limit", "top-by-volume.limit.max")
            }
        }
    }

    companion object {
        protected val log = LoggerFactory.getLogger(
            GetTopByVolumeValidator::class.java
        )
    }
}