package tech.edgx.prise.webserver.util

import org.springframework.http.HttpStatus
import org.springframework.validation.BindingResult
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "Issue with service")
class InvalidRequestException : RuntimeException {
    var errors: BindingResult

    constructor(t: Throwable?, errors: BindingResult) : super(t) {
        this.errors = errors
    }

    constructor(errors: BindingResult) {
        this.errors = errors
    }
}
