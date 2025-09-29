package tech.edgx.prise.indexer.util

import com.bloxbean.cardano.client.common.cbor.CborSerializationUtil
import com.bloxbean.cardano.client.plutus.spec.PlutusData
import com.bloxbean.cardano.yaci.core.util.HexUtil
import kotlin.test.Test

class Scratchpad {

    @Test
    fun getHashFromDatum() {
        val inlineDatum = "d8799fd8799fd87a9f581c1eae96baf29e27682ea3f815aba361a0c6059d45e4bfbe95bbd2f44affffd8799f4040ffd8799f581ce13f55c16b8718edac43614146c00cadc45991af3a5355d0386a9f034b43727970746f536f636b7aff1a11d0bf121b0000000360e9f95e1a166ea6fc18641864d8799f190682ffd87980ff"
        val plutusData = PlutusData.deserialize(CborSerializationUtil.deserialize(HexUtil.decodeHexString(inlineDatum)))
        println(plutusData.datumHash)
    }
}