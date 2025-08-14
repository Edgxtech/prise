package tech.edgx.prise.webserver.domain

import jakarta.persistence.*

interface Asset {
    var id: Long?
    var unit: String?
    var policy: String?
    var native_name: String?
    var sidechain: String?
    var decimals: Int?
    var logo_uri: String?
    var preferred_name: String?
    var metadata_fetched: Boolean?
}

@Entity
@Table(name = "asset")
class AssetImpl : Asset {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    override var id: Long? = null

    @Column(name = "unit")
    override var unit: String? = null

    @Column(name = "policy")
    override var policy: String? = null

    @Column(name = "native_name")
    override var native_name: String? = null

    @Column(name = "sidechain")
    override var sidechain: String? = null

    @Column(name = "decimals")
    override var decimals: Int? = null

    @Column(name = "logo_uri", length = 600)
    override var logo_uri: String? = null

    @Column(name = "preferred_name")
    override var preferred_name: String? = null

    @Column(name = "metadata_fetched")
    override var metadata_fetched: Boolean? = null
}
