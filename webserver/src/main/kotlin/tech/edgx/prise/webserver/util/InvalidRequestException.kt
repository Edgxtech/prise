package tech.edgx.prise.webserver.util

import org.springframework.validation.BindingResult

class InvalidRequestException : RuntimeException {
    val errors: BindingResult

    constructor(errors: BindingResult) : super("Invalid request parameters") {
        this.errors = errors
    }

    constructor(message: String) : super(message) {
        this.errors = org.springframework.validation.BeanPropertyBindingResult(null, "request")
        this.errors.addError(org.springframework.validation.ObjectError("request", message))
    }
}