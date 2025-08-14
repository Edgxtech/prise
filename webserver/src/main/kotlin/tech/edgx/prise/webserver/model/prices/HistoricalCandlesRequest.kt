package tech.edgx.prise.webserver.model.prices

import io.swagger.v3.oas.annotations.Hidden
import io.swagger.v3.oas.annotations.media.Schema
import jakarta.validation.constraints.NotBlank

@Schema(description = "Retrieve historical candlestick data for a Cardano Native Token pair")
data class HistoricalCandlesRequest(

    @Schema(
        description = "Asset/quote pair in the format 'asset_unit:quote_unit'. If only asset is provided, defaults to ':ADA'. Example: '279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b:ADA' or '279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b'.",
        type = "string",
        example = "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b",
        required = true
    )
    @NotBlank(message = "Symbol is required")
    val symbol: String,

    @Schema(
        description = "Resolution width (e.g., '15m' for 15 minutes, '1h' for 1 hour, '1D' for 1 day, '1W' for 1 week).",
        type = "string",
        example = "1D",
        required = true
    )
    @NotBlank(message = "Resolution is required")
    val resolution: String,

    @Schema(
        description = "Start time for historical data [Seconds since epoch]. Optional.",
        type = "long",
        example = "1735689600",
        required = false
    )
    var from: Long? = null,

    @Hidden
    @Schema(
        description = "End time for historical data [Seconds since epoch]. Optional.",
        type = "long",
        required = false
    )
    var to: Long? = null
)