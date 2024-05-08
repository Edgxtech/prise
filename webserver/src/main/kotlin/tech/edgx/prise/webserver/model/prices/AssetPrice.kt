package tech.edgx.prise.webserver.model.prices

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonInclude
import java.util.*

@JsonInclude(JsonInclude.Include.NON_NULL)
class AssetPrice(
    var symbol: String,
    var name: String,
    var last_price_usd: Double?,
    var last_price_ada: Double?,
    @field:JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ")
    var last_update: Date?,
    var pricing_provider: String?
)