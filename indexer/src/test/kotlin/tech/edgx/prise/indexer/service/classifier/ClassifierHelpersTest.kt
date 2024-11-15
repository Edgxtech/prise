package tech.edgx.prise.indexer.service.classifier

import com.bloxbean.cardano.client.plutus.spec.serializers.PlutusDataJsonConverter
import com.bloxbean.cardano.client.util.JsonUtil
import com.bloxbean.cardano.yaci.core.model.TransactionOutput
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import tech.edgx.prise.indexer.service.classifier.common.ClassifierHelpers

class ClassifierHelpersTest {

    @Test
    fun getPlutusData() {
        // Utxo: 1973d456714f3fc83fa02c6f3f9640290381ce82edbabd085ca06ca936ca64d7#1
        val inlineDatum="d8799fd8799fd87a9f581c1eae96baf29e27682ea3f815aba361a0c6059d45e4bfbe95bbd2f44affffd8799f4040ffd8799f581c29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6434d494eff1b00003ab15584f9871b00000c7b47c0262e1b0001195186ac7f08181e1864d8799f190d05ffd87980ff"
        val output = TransactionOutput.builder()
            .datumHash(null)
            .inlineDatum(inlineDatum)
            .build()
        val plutusData = ClassifierHelpers.getPlutusDataFromOutput(output, listOf())
        assertEquals(inlineDatum,plutusData?.serializeToHex())
        val datumJson = JsonUtil.parseJson(PlutusDataJsonConverter.toJson(plutusData))
        val aScriptHash = datumJson.get("fields")?.get(0)?.get("fields")?.get(0)?.get("fields")?.get(0)?.get("bytes")?.asText()
        val aPolicy = datumJson.get("fields")?.get(2)?.get("fields")?.get(0)?.get("bytes")?.asText()
        val aHexName = datumJson.get("fields")?.get(2)?.get("fields")?.get(1)?.get("bytes")?.asText()
        assertEquals("1eae96baf29e27682ea3f815aba361a0c6059d45e4bfbe95bbd2f44a", aScriptHash)
        assertEquals("29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6", aPolicy)
        assertEquals("4d494e", aHexName)
    }
}