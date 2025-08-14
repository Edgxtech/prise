package tech.edgx.prise.indexer.domain

import org.ktorm.entity.Entity
import org.ktorm.schema.*

object Assets : Table<Asset>("asset") {
    val id = long("id").primaryKey().bindTo { it.id }
    val policy = varchar("policy").bindTo { it.policy }
    val native_name = varchar("native_name").bindTo { it.native_name }
    val unit = varchar("unit").bindTo { it.unit }
    val sidechain = varchar("sidechain").bindTo { it.sidechain }
    val decimals = int("decimals").bindTo { it.decimals }
    val logo_uri = varchar("logo_uri").bindTo { it.logo_uri }
    val preferred_name = varchar("preferred_name").bindTo { it.preferred_name }
    val metadata_fetched = boolean("metadata_fetched").bindTo { it.metadata_fetched }
}

interface Asset : Entity<Asset> {
    companion object : Entity.Factory<Asset>()
    val id: Long
    var unit: String
    var policy: String
    var native_name: String
    var sidechain: String?
    var decimals: Int?
    var logo_uri: String?
    var preferred_name: String?
    var metadata_fetched: Boolean?
}