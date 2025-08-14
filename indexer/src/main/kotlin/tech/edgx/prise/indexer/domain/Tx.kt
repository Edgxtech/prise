package tech.edgx.prise.indexer.domain

import org.ktorm.entity.Entity
import org.ktorm.schema.Table
import org.ktorm.schema.bytes
import org.ktorm.schema.long

object Txs : Table<Tx>("tx") {
    val id = long("id").primaryKey().bindTo { it.id }
    val hash = bytes("hash").bindTo { it.hash }
}

interface Tx : Entity<Tx> {
    companion object : Entity.Factory<Tx>()
    var id: Long
    var hash: ByteArray
}