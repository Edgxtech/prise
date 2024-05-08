package tech.edgx.prise.webserver.domain

import javax.persistence.Column
import java.io.Serializable

class SymbolDateId : Serializable {
    @Column(name = "time")
    var time: Long? = null

    @Column(name = "symbol")
    var symbol: String? = null

    constructor()
    constructor(symbol: String?, time: Long?) {
        this.symbol = symbol
        this.time = time
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SymbolDateId) return false
        return symbol != null && symbol == other.symbol && time != null && time == other.time
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }
}