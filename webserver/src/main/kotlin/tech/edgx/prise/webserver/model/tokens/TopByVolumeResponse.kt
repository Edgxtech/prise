package tech.edgx.prise.webserver.model.tokens

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.LocalDateTime

data class TopByVolumeResponse(
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    var date: LocalDateTime,
    var assets: Set<AssetResponse>
)