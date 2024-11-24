package tech.edgx.prise.webserver.model.prices

import com.fasterxml.jackson.annotation.JsonFormat
import java.util.*

data class LatestPricesResponse(
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ")
    var date: Date,
    var assets: List<AssetPrice>
)