package tech.edgx.prise.webserver.model.prices

import io.swagger.v3.oas.annotations.media.Schema
import tech.edgx.prise.webserver.util.Helpers

@Schema(description = "Retrieve latest prices of Cardano Native Tokens")
data class LatestPricesRequest(
    @Schema(
        description = "List of asset/quote pairs (e.g., 'asset_unit:quote_unit'). If empty, returns latest prices for all pairs. Quote defaults to 'ADA' if not specified.",
        example = "[\"279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b\", \"533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459\", \"da8c30857834c6ae7203935b89278c532b3995245295456f993e1d244c51\"]",
        type = "set",
        required = false
    )
    val symbols: Set<String> = emptySet(),

    @Schema(hidden = true)
    val network: Int = Helpers.MAINNET_ID
)