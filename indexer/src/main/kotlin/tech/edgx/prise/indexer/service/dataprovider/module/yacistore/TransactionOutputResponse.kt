package tech.edgx.prise.indexer.service.dataprovider.module.yacistore

import com.google.gson.annotations.SerializedName

data class UtxoDetails (
    @SerializedName("block_number"             ) var blockNumber            : Int?               = null,
    @SerializedName("block_time"               ) var blockTime              : Int?               = null,
    @SerializedName("tx_hash"                  ) var txHash                 : String?            = null,
    @SerializedName("output_index"             ) var outputIndex            : Int?               = null,
    @SerializedName("slot"                     ) var slot                   : Int?               = null,
    @SerializedName("block_hash"               ) var blockHash              : String?            = null,
    @SerializedName("epoch"                    ) var epoch                  : Int?               = null,
    @SerializedName("owner_addr"               ) var ownerAddr              : String?            = null,
    @SerializedName("owner_stake_addr"         ) var ownerStakeAddr         : String?            = null,
    @SerializedName("owner_payment_credential" ) var ownerPaymentCredential : String?            = null,
    @SerializedName("owner_stake_credential"   ) var ownerStakeCredential   : String?            = null,
    @SerializedName("lovelace_amount"          ) var lovelaceAmount         : Long?               = null,
    @SerializedName("amounts"                  ) var amounts                : ArrayList<Amounts> = arrayListOf(),
    @SerializedName("data_hash"                ) var dataHash               : String?            = null,
    @SerializedName("inline_datum"             ) var inlineDatum            : String?            = null,
    @SerializedName("script_ref"               ) var scriptRef              : String?            = null,
    @SerializedName("reference_script_hash"    ) var referenceScriptHash    : String?            = null,
    @SerializedName("is_collateral_return"     ) var isCollateralReturn     : String?            = null

)

data class Amounts (
    @SerializedName("unit"       ) var unit      : String? = null,
    @SerializedName("policy_id"  ) var policyId  : String? = null,
    @SerializedName("asset_name" ) var assetName : String? = null,
    @SerializedName("quantity"   ) var quantity  : Long?    = null
)