package tech.edgx.prise.indexer.testutil

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonInclude
import java.math.BigDecimal
import java.util.*

@JsonInclude(JsonInclude.Include.NON_NULL)
class RealfiAssetPrice(
    var id: String,
    var symbol: String,
    var last_price_usd: Double?,
    var last_price_ada: Double?,
    @field:JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ")
    var last_update: Date?,
    var pricing_provider: String?
)
