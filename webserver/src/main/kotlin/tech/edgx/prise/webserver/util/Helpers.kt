package tech.edgx.prise.webserver.util

import org.slf4j.LoggerFactory
import java.time.*

object Helpers {
    internal val log = LoggerFactory.getLogger(Helpers::class.java)
    var TESTNET_ID = 0
    var MAINNET_ID = 1

    const val RESO_DEFN_1W = "1W"
    const val RESO_DEFN_1D = "1D"
    const val RESO_DEFN_1H = "1h"
    const val RESO_DEFN_15M = "15m"

    // Helper function to align to Monday midnight UTC
    fun alignToMonday(time: Long): Long {
        val instant = Instant.ofEpochSecond(time)
        val zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        // Find the previous or current Monday (ISO week: Monday = 1, Sunday = 7)
        val daysSinceMonday = (zonedDateTime.dayOfWeek.value - 1) % 7 // Monday = 0, Tuesday = 1, ..., Sunday = 6
        val mondayMidnight = zonedDateTime.minusDays(if (daysSinceMonday >= 0) daysSinceMonday.toLong() else (daysSinceMonday + 7).toLong())
            .withHour(0).withMinute(0).withSecond(0).withNano(0)
        return mondayMidnight.toEpochSecond()
    }
}
