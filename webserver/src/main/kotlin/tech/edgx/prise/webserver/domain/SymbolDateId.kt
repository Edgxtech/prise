package tech.edgx.prise.webserver.domain

import java.io.Serializable
import jakarta.persistence.*

@Embeddable
data class SymbolDateId(
    @Column(name = "time")
    var time: Long? = null,

    @Column(name = "asset_id")
    var assetId: Long? = null,

    @Column(name = "quote_asset_id")
    var quoteAssetId: Long? = null

) : Serializable {
    // Required for JPA embeddable composite keys
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SymbolDateId) return false
        return time == other.time && assetId == other.assetId && quoteAssetId == other.quoteAssetId
    }

    override fun hashCode(): Int {
        return 31 * (time?.hashCode() ?: 0) + (assetId?.hashCode() ?: 0) + (quoteAssetId?.hashCode() ?: 0)
    }
}