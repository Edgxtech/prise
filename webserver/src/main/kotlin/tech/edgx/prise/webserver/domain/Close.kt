package tech.edgx.prise.webserver.domain

import java.io.Serializable

interface Close : Serializable {
    var time: Long?
    var close: Double?
}