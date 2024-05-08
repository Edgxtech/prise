package tech.edgx.prise.webserver.domain

import javax.persistence.*
import java.util.*

@Entity
@Table(name = "ASSET")
class AssetImpl : Asset {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    override var id: Long? = null

    /* cardano name (decoded hex) */
    override var native_name: String? = null

    /* unit (concat policyid + hex encoded name) */
    override var unit: String? = null
    override var price: Double? = null
    override var ada_price: Double? = null
    override var decimals: Int? = null
    override var last_price_update: Date? = null
    override var policy: String? = null
    override var preferred_name: String? = null

    @Column(length = 600)
    override var logo_uri: String? = null
    override var sidechain: String? = null
    override var pricing_provider: String? = null
    override var incomplete_price_data: Boolean? = null
}