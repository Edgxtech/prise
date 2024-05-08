package tech.edgx.prise.indexer.service.dataprovider.module.carp.http.model

//data class TransactionOutputResponse(
//    val utxos: List<UtxoAndBlockInfo>
//)

data class UtxoAndBlockInfo(
    val utxo: PointerWithPayload
)

//export type UtxoAndBlockInfo = {
//    block: BlockInfo;
//    utxo: UtxoPointer & {
//        /**
//         * @pattern [0-9a-fA-F]*
//         * @example "825839019cb581f4337a6142e477af6e00fe41b1fc4a5944a575681b8499a3c0bd07ce733b5911eb657e7aff5d35f8b0682fe0380f7621af2bbcb2f71b0000000586321393"
//         */
//        payload: string;
//    };
//};
//export type TransactionOutputResponse = {
//    utxos: UtxoAndBlockInfo[];
//};

//lockInfo extends this;..
//export type BlockSubset = {
//    /**
//     * @example 1
//     */
//    era: number;
//    /**
//     * @pattern [0-9a-fA-F]{64}
//     * @example "cf8c63a909d91776e27f7d05457e823a9dba606a7ab499ac435e7904ee70d7c8"
//     */
//    hash: string;
//    /**
//     * @example 4512067
//     */
//    height: number;
//    /**
//     * @example 209
//     */
//    epoch: number;
//    /**
//     * @example 4924800
//     */
//    slot: number;
//};