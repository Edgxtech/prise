package tech.edgx.prise.indexer.testutil

import com.fasterxml.jackson.annotation.JsonFormat
import java.util.*

class LatestPricesResponse {
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ")
    var date: Date? = null
    var assets: List<RealfiAssetPrice>? = null
}
