package tech.edgx.prise.webserver.repository

import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.stereotype.Repository
import tech.edgx.prise.webserver.domain.*

@Repository
interface CandleWeeklyRepository : CrudRepository<CandleWeeklyImpl?, SymbolDateId?> {
    @Query("select c from CandleWeeklyImpl c where c.symbol = ?1 and c.time > ?2 and c.time < ?3")
    fun myFindByIdFromTo(symbol: String?, from: Long?, to: Long?): List<Candle?>?

    /* For some reason, cant map this with just the time,close params to a custom view, might be a mysql thing, revisit later */
    @Query("select c from CandleWeeklyImpl c where c.symbol = ?1 and c.time > ?2 and c.time < ?3")
    fun myFindClosesByIdFromTo(symbol: String?, from: Long?, to: Long?): List<Close?>?

}