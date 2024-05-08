package tech.edgx.prise.webserver.model.prices

import io.swagger.annotations.ApiModel
import io.swagger.annotations.ApiModelProperty
import tech.edgx.prise.webserver.util.Helpers

@ApiModel(value = "Retrieve latest prices of Cardano Native Tokens")
class LatestPricesRequest {

    @ApiModelProperty(
        value = "Cardano Native Token Id (hexpolicy+hexname) or (lovelace) for ADA",
        example = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459,279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b,lovelace",
        dataType = "set",
        required = false)
    var symbol: Set<String> = HashSet()

    /* Not a user option for the time being */
    @ApiModelProperty(hidden = true)
    var network: Int = Helpers.MAINNET_ID
}