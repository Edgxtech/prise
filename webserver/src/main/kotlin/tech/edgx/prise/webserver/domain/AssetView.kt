package tech.edgx.prise.webserver.domain

import java.io.Serializable
import java.util.*

interface AssetView : Serializable {
    val unit: String
    val price: Double?
    val ada_price: Double?
    val last_price_update: Date?
    val pricing_provider: String?
}