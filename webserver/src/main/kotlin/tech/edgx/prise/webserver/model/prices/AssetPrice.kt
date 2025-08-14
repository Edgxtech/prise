package tech.edgx.prise.webserver.model.prices

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
data class AssetPrice(
    var asset: String,
    var quote: String,
    var name: String,
    var last_price: Double?,
    val last_update: Long? = null,
    var provider: String?,
    @JsonIgnore
    val asset_id: Long? = null,
    @JsonIgnore
    val quote_asset_id: Long? = null
)