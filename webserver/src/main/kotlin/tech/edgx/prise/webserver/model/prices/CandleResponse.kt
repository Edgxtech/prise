package tech.edgx.prise.webserver.model.prices

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable

data class CandleResponse(
    @JsonProperty("time") val time: Long? = null,
    @JsonProperty("open") val open: Double? = null,
    @JsonProperty("high") val high: Double? = null,
    @JsonProperty("low") val low: Double? = null,
    @JsonProperty("close") val close: Double? = null,
    @JsonProperty("volume") val volume: Double = 0.0
) : Serializable