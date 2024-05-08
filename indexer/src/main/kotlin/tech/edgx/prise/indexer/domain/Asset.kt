package tech.edgx.prise.indexer.domain

import org.ktorm.entity.Entity
import org.ktorm.schema.*
import java.time.LocalDateTime

object Assets : Table<Asset>("asset") {
    val id = long("id").primaryKey().bindTo { it.id }
    val policy = varchar("policy").bindTo { it.policy }
    val native_name = varchar("native_name").bindTo { it.native_name }
    val unit = varchar("unit").bindTo { it.unit }
    val price = double("price").bindTo { it.price }
    val ada_price = double("ada_price").bindTo { it.ada_price }
    val sidechain = varchar("sidechain").bindTo { it.sidechain }
    val decimals = int("decimals").bindTo { it.decimals }
    val incomplete_price_data = boolean("incomplete_price_data").bindTo { it.incomplete_price_data }
    val last_price_update = datetime("last_price_update").bindTo { it.last_price_update }
    val pricing_provider = varchar("pricing_provider").bindTo { it.pricing_provider }
    val logo_uri = varchar("logo_uri").bindTo { it.logo_uri }
    val preferred_name = varchar("preferred_name").bindTo { it.preferred_name }
}

interface Asset : Entity<Asset> {
    companion object : Entity.Factory<Asset>()
    val id: Long
    var policy: String
    var native_name: String
    var unit: String
    var price: Double?
    var ada_price: Double?
    var sidechain: String?
    var decimals: Int?
    var incomplete_price_data: Boolean?
    var last_price_update: LocalDateTime?
    var pricing_provider: String?
    var logo_uri: String?
    var preferred_name: String?
}