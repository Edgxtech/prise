package tech.edgx.prise.webserver.model.prices
import io.swagger.annotations.ApiModel
import io.swagger.annotations.ApiModelProperty

@ApiModel(value = "Retrieve historical prices of Cardano Native Tokens")
data class PriceHistoryRequest(

    @ApiModelProperty(
        position = 1,
        value = "Cardano Native Token Id (hexpolicy+hexname); e.g. 279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b",
        dataType = "string",
        example = "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b",
        required = true)
    val symbol: String = "",

    @ApiModelProperty(
        value = "Resolution width",
        example = "1D",
        dataType = "string",
        required = true)
    val resolution: String = "",

    @ApiModelProperty(
        value = "Time from [Seconds since epoch]",
        dataType = "long",
        required = false)
    var from: Long?,

    @ApiModelProperty(
        value = "Time from [Seconds since epoch]",
        dataType = "long",
        required = false)
    var to: Long?,
)