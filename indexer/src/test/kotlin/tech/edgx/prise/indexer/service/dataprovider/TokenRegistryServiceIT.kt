package tech.edgx.prise.indexer.service.dataprovider

import com.google.gson.Gson
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import tech.edgx.prise.indexer.model.dataprovider.Decimals
import tech.edgx.prise.indexer.model.dataprovider.SubjectDecimalPair
import tech.edgx.prise.indexer.service.BaseIT
import tech.edgx.prise.indexer.testutil.TestHelpers
import java.io.File
import java.util.*

class TokenRegistryServiceIT: BaseIT() {

    val tokenMetadataService: TokenMetadataService by inject(named("tokenRegistryNativeTokenDataGetter"))

    @Test
    fun bulkGetDecimals() {
        val response: List<SubjectDecimalPair>? = tokenMetadataService.getDecimals(TestHelpers.test_units_a)
        val decimals: List<Decimals>? = response?.map { r -> r.decimals!! }
        val vals: List<Int> = decimals?.map { d -> d.value!! }?.toList() ?: listOf()
        println("Decimal vals: $vals")
        assertTrue(vals == listOf(6, 6, 6, 0, 0, 6))
    }

    @Test
    fun bulkGetDecimalsAllTokens() {
        val jsonString: String = File("src/test/resources/testdata/all_units.json").readText(Charsets.UTF_8)
        val allUnits = Gson().fromJson(jsonString, Array<String>::class.java).asList()
        val chunkedUnits: List<List<String>> = allUnits.chunked(70) /* Max # appears to be 80-90 */
        assertDoesNotThrow {
            chunkedUnits.forEach { cu ->
                val response: List<SubjectDecimalPair>? = tokenMetadataService.getDecimals(cu)
                println("Response: $response")
            }
        }
    }
}