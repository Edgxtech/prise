package tech.edgx.prise.webserver.model.prices

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable

data class CloseResponse(
    @JsonProperty("time") val time: Long,
    @JsonProperty("close") val close: Double
) : Serializable