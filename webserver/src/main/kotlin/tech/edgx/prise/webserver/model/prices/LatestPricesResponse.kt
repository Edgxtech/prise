package tech.edgx.prise.webserver.model.prices

import com.fasterxml.jackson.annotation.JsonFormat
import java.util.*

class LatestPricesResponse {
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ")
    lateinit var date: Date
    lateinit var assets: List<AssetPrice>
}