package tech.edgx.prise.webserver.model.tokens

import com.fasterxml.jackson.annotation.JsonInclude
import io.swagger.v3.oas.annotations.media.Schema
import java.io.Serializable

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Asset with volume information for API response")
data class AssetResponse(
    @Schema(description = "Unit of the asset", example = "ADA")
    val unit: String?,

    @Schema(description = "Native name of the asset", example = "Cardano")
    val nativeName: String?,

    @Schema(description = "Preferred name of the asset", example = "Cardano")
    val preferredName: String?,

    @Schema(description = "Total volume in the last 24 hours", example = "1000000.50")
    val totalVolume: Double?
): Serializable