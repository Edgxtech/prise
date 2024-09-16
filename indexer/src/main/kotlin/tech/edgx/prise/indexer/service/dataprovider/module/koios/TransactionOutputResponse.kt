package tech.edgx.prise.indexer.service.dataprovider.module.koios

data class UtxoDetails (
    val tx_hash: String,
    val tx_index: Int,
    val address: String,
    val value: Long,
    val stake_address: String,
    val payment_cred: String,
    val epoch_no: Int,
    val block_height: Long,
    val block_time: Long,
    val datum_hash: String,
    val inline_datum: InlineDatum?,
    val reference_script: String,
    val asset_list: List<Asset>,
    val is_spent: Boolean
)

data class Asset (
    val decimals: Int,
    val quantity: Long,
    val policy_id: String,
    val asset_name: String,
    val fingerprint: String
)

data class InlineDatum (
    val bytes: String
)
