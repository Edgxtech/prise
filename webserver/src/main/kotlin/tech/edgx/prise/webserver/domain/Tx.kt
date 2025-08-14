package tech.edgx.prise.webserver.domain

import jakarta.persistence.*

interface Tx {
    var id: Long?
    var hash: ByteArray?
}

@Entity
@Table(name = "tx")
class TxImpl : Tx {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    override var id: Long? = null

    @Column(name = "hash", nullable = false, length = 32)
    override var hash: ByteArray? = null
}