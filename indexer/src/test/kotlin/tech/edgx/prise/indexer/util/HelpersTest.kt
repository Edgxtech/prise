package tech.edgx.prise.indexer.util

import com.bloxbean.cardano.client.util.HexUtil
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import tech.edgx.prise.indexer.model.DexEnum
import java.time.LocalDateTime

class HelpersTest {

    @Test
    fun regexpMatchPolicy() {
        val r = "^\\p{XDigit}{64}+$".toRegex()
        assertTrue(r.matches("69837fca12d5d8fe4011866e20a5377e849f7611042fb460a96fde55838db8ec"))
    }

    @Test
    fun convertAddressToCredential() {
        val cred = Helpers.convertScriptAddressToPaymentCredential("addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxzvuxnvpk6sacr650lmfeqmmpxvhes2nq89lcvvgpq786n6qrzzw73")
        println("Credential: $cred")
        assertEquals("e1317b152faac13426e6a83e06ff88a4d62cce3c1634ab0a5ec13309", cred)
    }

    @Test
    fun calculatePriceInOtherAsset() {
        /* decimals, ada amount (always decimal 6), token amount */
        var price = Helpers.calculatePriceInAsset1(1000000, 6.0,2000000, 6.0)
        println(price)
        assertTrue(price?.equals(0.5)?: false)
        price = Helpers.calculatePriceInAsset1(1000000, 6.0, 20, 0.0)
        println(price)
        assertTrue(price?.equals(0.05)?: false)
        price = Helpers.calculatePriceInAsset1(1000000, 6.0,5, 0.0)
        println(price)
        assertTrue(price?.equals(0.2)?: false)
        price = Helpers.calculatePriceInAsset1( 1000000, 6.0,20000, 3.0)
        println(price)
        assertTrue(price?.equals(0.05)?: false)
        price = Helpers.calculatePriceInAsset1( 50000000, 6.0,10000, 3.0)
        println(price)
        assertTrue(price?.equals(5.0)?: false)
    }

    @Test
    fun getDexName() {
        assertTrue(Helpers.getDexName(0) == DexEnum.WINGRIDERS.nativeName)
        assertTrue(Helpers.getDexName(1) == DexEnum.SUNDAESWAP.nativeName)
        assertTrue(Helpers.getDexName(2) == DexEnum.MINSWAP.nativeName)
        assertTrue(Helpers.getDexName(3) == DexEnum.MINSWAPV2.nativeName)
        assertTrue(Helpers.getDexName(4) == DexEnum.SATURNSWAP.nativeName)
        assertThrows(Exception::class.java) { Helpers.getDexName(5) }
        assertThrows(Exception::class.java) { Helpers.getDexName(-1) }
    }

    @Test
    fun decodeHexString() {
        assertTrue(String(HexUtil.decodeHexString("534e454b")) == "SNEK")
        assertTrue(String(HexUtil.decodeHexString("494e4459")) == "INDY")
        assertTrue(String(HexUtil.decodeHexString("57696e67526964657273"))=="WingRiders")
    }

    @Test
    fun findEarliestDate() {
        val earliest = LocalDateTime.now()
        val lastWeeklyDate = earliest.minusDays(4)
        val lastDailyDate = earliest.minusDays(5)
        val lastHourlyDate = earliest.minusDays(6)
        val lastFifteenDate = earliest.minusDays(7)
        val earliestB = listOf<LocalDateTime>(
            LocalDateTime.now(), lastWeeklyDate, lastDailyDate, lastHourlyDate, lastFifteenDate)
            .min()
            .minusMonths(1)
        println("Earliest: $earliestB")
        assertTrue(earliestB.equals(lastFifteenDate.minusMonths(1)))
    }

    @Test
    fun regexpMatchBlockfrostApiKey() {
        val r = """^mainnet[A-Za-z0-9_-]{32}+""".toRegex()
        assertTrue(r.matches("mainnet1234567890abcdefghijklmnopqrstuv"))
    }
}