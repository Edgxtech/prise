package tech.edgx.prise.webserver.model.prices

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.LocalDateTime

data class LatestPricesResponse(
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    var date: LocalDateTime,
    var assets: List<AssetPrice>
)