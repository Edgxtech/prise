package tech.edgx.prise.indexer.service.dataprovider

import com.google.gson.Gson
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import tech.edgx.prise.indexer.model.dataprovider.SubjectDecimalPair
import tech.edgx.prise.indexer.Base
import tech.edgx.prise.indexer.service.dataprovider.common.TokenMetadataServiceEnum
import tech.edgx.prise.indexer.testutil.TestHelpers
import java.io.File
import java.util.*

class TokenRegistryServiceIT: Base() {

    val tokenMetadataService: TokenMetadataService by inject(named(TokenMetadataServiceEnum.tokenRegistry.name)) //"tokenRegistryNativeTokenDataGetter"

    @Test
    fun bulkGetDecimals() {
        val response: List<SubjectDecimalPair> = tokenMetadataService.getDecimals(TestHelpers.test_units_a.map { it.first })
        //val decimals: List<Decimals> = response.map { r -> r.decimals!! }
        TestHelpers.test_units_a.forEach { testToken ->
            val responseForToken = response.filter { it.subject==testToken.first }.firstOrNull()
            println("TestToken: $testToken VS Response: $responseForToken")
            if (responseForToken == null && testToken.second == null) {
                assertEquals(testToken.second, null)
            } else {
                assertEquals(testToken.second, responseForToken?.decimals?.value)
            }
        }
    }

    @Test
    fun bulkGetDecimalsAllTokens() {
        val jsonString: String = File("src/test/resources/testdata/subset_units.json").readText(Charsets.UTF_8)
        val allUnits = Gson().fromJson(jsonString, Array<String>::class.java).asList()
        val chunkedUnits: List<List<String>> = allUnits.take(145).chunked(70) /* Max # appears to be 80-90 */
        assertDoesNotThrow {
            chunkedUnits.forEach { cu ->
                val response: List<SubjectDecimalPair> = tokenMetadataService.getDecimals(cu)
                println("Response: $response")
            }
        }
    }
}