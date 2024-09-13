package tech.edgx.prise.indexer.service.classifier.common

import com.bloxbean.cardano.client.common.cbor.CborSerializationUtil
import com.bloxbean.cardano.client.plutus.spec.PlutusData
import com.bloxbean.cardano.yaci.core.model.Datum
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import com.bloxbean.cardano.yaci.core.util.HexUtil

object ClassifierHelpers {

    /* Extract the bloxb client lib PlutusData related to the input */
    fun getPlutusDataFromOutput(output: TransactionOutput, transactionDatums: List<Datum>): PlutusData? {
        return when(output.inlineDatum!=null) {
            true -> {
                PlutusData.deserialize(CborSerializationUtil.deserialize(HexUtil.decodeHexString(output.inlineDatum)))
            }
            false -> {
                transactionDatums.firstOrNull {
                    val datumHash = PlutusData.deserialize(HexUtil.decodeHexString(it.cbor)).datumHash
                    datumHash == output.datumHash }?.cbor
                ?.let {
                    PlutusData.deserialize(CborSerializationUtil.deserialize(HexUtil.decodeHexString(it)))
                }
            }
        }
    }
}