package tech.edgx.prise.webserver.util

import org.apache.commons.logging.LogFactory

object Helpers {
    internal val log = LogFactory.getLog(Helpers::class.java)
    var TESTNET_ID = 0
    var MAINNET_ID = 1

    const val RESO_DEFN_1W = "1W"
    const val RESO_DEFN_1D = "1D"
    const val RESO_DEFN_4H = "4h"
    const val RESO_DEFN_2H = "2h"
    const val RESO_DEFN_1H = "1h"
    const val RESO_DEFN_30M = "30m"
    const val RESO_DEFN_15M = "15m"

    const val ADA_ASSET_UNIT = "lovelace"
}
