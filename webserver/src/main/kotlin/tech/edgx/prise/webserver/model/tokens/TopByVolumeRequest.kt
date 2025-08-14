package tech.edgx.prise.webserver.model.tokens

import io.swagger.v3.oas.annotations.media.Schema
import tech.edgx.prise.webserver.util.Helpers

@Schema(description = "Retrieve Top Tokens By 24h Volume")
data class TopByVolumeRequest(

    @Schema(
        description = "Limit of tokens returned when order by volume",
        example = "10",
        type = "int",
        required = true
    )
    val limit: Int = 100,

    @Schema(hidden = true)
    val network: Int = Helpers.MAINNET_ID
)