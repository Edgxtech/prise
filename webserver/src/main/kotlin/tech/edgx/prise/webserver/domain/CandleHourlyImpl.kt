package tech.edgx.prise.webserver.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.IdClass
import javax.persistence.Table

@Entity
@IdClass(SymbolDateId::class)
@Table(name = "CANDLE_HOURLY")
class CandleHourlyImpl : Candle {
    @Id
    override var time: Long? = null

    @Id
    @JsonIgnore
    override var symbol: String? = null
    override var open: Double? = null
    override var high: Double? = null
    override var low: Double? = null
    override var close: Double? = null
    override var volume: Double? = null

    constructor()
    constructor(
        symbol: String?,
        time: Long?,
        open: Double?,
        high: Double?,
        low: Double?,
        close: Double?,
        volume: Double?
    ) {
        this.symbol = symbol
        //this.dtg = dtg;
        this.time = time
        this.open = open
        this.high = high
        this.low = low
        this.close = close
        this.volume = volume
    }
}