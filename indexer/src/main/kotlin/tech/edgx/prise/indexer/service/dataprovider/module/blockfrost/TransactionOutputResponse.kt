package tech.edgx.prise.indexer.service.dataprovider.module.blockfrost

import com.google.gson.annotations.SerializedName

data class Transaction (
    @SerializedName("hash"    ) var hash    : String?            = null,
    @SerializedName("inputs"  ) var inputs  : ArrayList<Inputs>  = arrayListOf(),
    @SerializedName("outputs" ) var outputs : ArrayList<Outputs> = arrayListOf()
)

data class Amount (
    @SerializedName("unit"     ) var unit     : String? = null,
    @SerializedName("quantity" ) var quantity : String? = null
)

data class Inputs (
    @SerializedName("address"               ) var address             : String?           = null,
    @SerializedName("amount"                ) var amount              : ArrayList<Amount> = arrayListOf(),
    @SerializedName("tx_hash"               ) var txHash              : String?           = null,
    @SerializedName("output_index"          ) var outputIndex         : Int?              = null,
    @SerializedName("data_hash"             ) var dataHash            : String?           = null,
    @SerializedName("inline_datum"          ) var inlineDatum         : String?           = null,
    @SerializedName("reference_script_hash" ) var referenceScriptHash : String?           = null,
    @SerializedName("collateral"            ) var collateral          : Boolean?          = null,
    @SerializedName("reference"             ) var reference           : Boolean?          = null
)

data class Outputs (
    @SerializedName("address"               ) var address             : String?           = null,
    @SerializedName("amount"                ) var amount              : ArrayList<Amount> = arrayListOf(),
    @SerializedName("output_index"          ) var outputIndex         : Int?              = null,
    @SerializedName("data_hash"             ) var dataHash            : String?           = null,
    @SerializedName("inline_datum"          ) var inlineDatum         : String?           = null,
    @SerializedName("collateral"            ) var collateral          : Boolean?          = null,
    @SerializedName("reference_script_hash" ) var referenceScriptHash : String?           = null,
    @SerializedName("consumed_by_tx"        ) var consumedByTx        : String?           = null
)