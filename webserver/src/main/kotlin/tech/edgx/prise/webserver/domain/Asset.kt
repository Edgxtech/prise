package tech.edgx.prise.webserver.domain

import java.util.*

interface Asset {
    var id: Long?
    var native_name: String?
    var unit: String?
    var price: Double?
    var ada_price: Double?
    var decimals: Int?
    var last_price_update: Date?
    var policy: String?
    var preferred_name: String?
    var logo_uri: String?
    var sidechain: String?
    var pricing_provider: String?
    var incomplete_price_data: Boolean?
}