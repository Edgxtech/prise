package tech.edgx.prise.indexer.util

import com.bloxbean.cardano.client.crypto.Bech32
import com.bloxbean.cardano.client.util.HexUtil
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.service.classifier.module.*
import java.math.BigDecimal
import java.math.BigInteger
import java.time.*
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters
import javax.xml.bind.DatatypeConverter
import kotlin.math.pow

object Helpers {

    private val log = LoggerFactory.getLogger(javaClass)

    val ASSET_LIVE_SYNC_LABEL = "asset.live.sync.count"
    val HISTORICAL_CANDLE_MAKE_COUNT = "historical.candle.make.count"
    val CHAIN_SYNC_SLOT_LABEL = "chain.sync.slot"

    const val RESO_DEFN_1W = "1W"
    const val RESO_DEFN_1D = "1D"
    const val RESO_DEFN_4H = "4h"
    const val RESO_DEFN_2H = "2h"
    const val RESO_DEFN_1H = "1h"
    const val RESO_DEFN_30M = "30m"
    const val RESO_DEFN_15M = "15m"

    const val ADA_ASSET_UNIT = "lovelace"
    const val ADA_ASSET_NAME = "ada"
    const val ADA_ASSET_DECIMALS = 6

    //const val CANDLE_PERSIST_BATCH_SIZE = 100

    val adaAsset = Asset.invoke {
        unit = ADA_ASSET_UNIT
        native_name = ADA_ASSET_NAME
        decimals = ADA_ASSET_DECIMALS
    }

    val zoneOffset: ZoneOffset = ZoneId.systemDefault().rules.getOffset(LocalDateTime.now())
    const val slotConversionOffset = -1591566291L

    /* Jan 12, 2022 06:01:01 - First ever dex swap
    *  Jan 12, 2022 06:00:00, 1641967200 - chosen start point */
    val dexLaunchTime: Long = 1641967200

//    val dexLaunchAdjustedBlockHash: String = "aa937b19ea01809f00c51994f21ca02ccda03618806c265d9c605d4834c07595"
//    val dexLaunchAdjustedBlockSlot: Long = 50400919L
    val dexLaunchAdjustedBlockHash: String = "2d5bf7d3f65a5b4ca8216ef05a3bd794a2350eaad327cf138f31e2238332edd7"
    val dexLaunchAdjustedBlockSlot: Long = 50400895L

    val allResoDurations = HistoricalCandleResolutions.entries.map { convertResoDuration(it.code) }
    val smallestDuration = allResoDurations.min()

    fun convertSlotToDtg(slot: Long): LocalDateTime {
        return LocalDateTime.ofEpochSecond(slot - slotConversionOffset, 0, zoneOffset)
    }

    fun calculatePriceInAsset1(amount1: Long?, decimals1: Double?, amount2: Long?, decimals2: Double?): Double? {
        if (amount1 != null && amount2 != null) {
            val amount1_dec_adjusted = if (decimals1 != null && decimals1 != 0.0) amount1.div(10.0.pow(decimals1)) else amount1.toDouble()
            val amount2_dec_adjusted = if (decimals2 != null && decimals2 != 0.0) amount2.div(10.0.pow(decimals2)) else amount2.toDouble()
            return amount1_dec_adjusted.div(amount2_dec_adjusted)
        }
        return null
    }

    fun calculatePriceInAsset1(amount1: BigInteger?, decimals1: Int?, amount2: BigInteger?, decimals2: Int?): Double? {
        if (amount1 != null && amount2 != null) {
            val amount1_dec_adjusted = if (decimals1 != null && decimals1 != 0) amount1.toDouble().div(10.0.pow(decimals1)) else amount1.toDouble()
            val amount2_dec_adjusted = if (decimals2 != null && decimals2 != 0) amount2.toDouble().div(10.0.pow(decimals2)) else amount2.toDouble()
            return amount1_dec_adjusted.div(amount2_dec_adjusted)
        }
        return null
    }

    fun calculatePriceInAsset1(amount1: BigDecimal, decimals1: Int, amount2: BigDecimal, decimals2: Int): Float {
        val amount1_dec_adjusted = if (decimals1 != 0) amount1.toDouble().div(10.0.pow(decimals1)) else amount1.toDouble()
        val amount2_dec_adjusted = if (decimals2 != 0) amount2.toDouble().div(10.0.pow(decimals2)) else amount2.toDouble()
        return amount1_dec_adjusted.div(amount2_dec_adjusted).toFloat()
    }

    fun getDexName(dexNumber: Int): String = DexEnum.fromId(dexNumber).nativeName
    fun getDexNumber(dexName: String): Int = DexEnum.fromName(dexName).code

    fun convertResoDuration(resolution: String?): Duration {
        val resoDuration = when (resolution) {
            RESO_DEFN_1W -> Duration.ofDays(7)
            RESO_DEFN_1D -> Duration.ofDays(1)
            RESO_DEFN_4H -> Duration.ofHours(4)
            RESO_DEFN_2H -> Duration.ofHours(2)
            RESO_DEFN_1H -> Duration.ofHours(1)
            RESO_DEFN_30M -> Duration.ofMinutes(30)
            RESO_DEFN_15M -> Duration.ofMinutes(15)
            else -> Duration.ofHours(2)
        }
        return resoDuration
    }

    /* Find the candle group date for a given price date */
    fun toNearestDiscreteDate(interval: Duration, dateTime: LocalDateTime): LocalDateTime {
        val zoneRules = ZoneId.systemDefault().rules
        val zoneOffset = zoneRules.getOffset(dateTime)
        if (interval == Duration.ofDays(7)) {
            return dateTime
                .truncatedTo(ChronoUnit.DAYS)
                .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
                .plusSeconds(zoneOffset.totalSeconds.toLong())
        }
        val instant = dateTime.toInstant(zoneOffset)
        val intervalMillis = interval.toMillis()
        val adjustedInstantMillis = instant.toEpochMilli() / intervalMillis * intervalMillis
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(adjustedInstantMillis), zoneOffset)
    }

    fun convertScriptAddressToPaymentCredential(address: String): String? {
        log.trace("Converting address: $address")
        if (!Bech32.isValid(address)) {
            return null
        }
        val addrData = Bech32.decode(address)
        val addressHex = HexUtil.encodeHexString(addrData.data)
        return when (addressHex.length >= 58) {
            true -> addressHex.substring(2, 58)
            else -> addressHex
        }
    }

    fun convertScriptAddressToHex(address: String): String? {
        log.trace("Converting address: $address")
        return when (Bech32.isValid(address)) {
            true -> {
                val addrData = Bech32.decode(address)
                HexUtil.encodeHexString(addrData.data)
            }
            else -> {
                null
            }
        }
    }

    fun resolveDexNumFromCredential(credential: String?): Int {
        return when (credential) {
            WingridersClassifier.POOL_SCRIPT_HASH -> DexEnum.WINGRIDERS.code
            SundaeswapClassifier.POOL_SCRIPT_HASH -> DexEnum.SUNDAESWAP.code
            in MinswapClassifier.POOL_SCRIPT_HASHES -> DexEnum.MINSWAP.code
            in MinswapV2Classifier.POOL_SCRIPT_HASHES -> DexEnum.MINSWAPV2.code
            else -> -1
        }
    }

    fun getRandomNumber(min: Int, max: Int): Int {
        return (Math.random() * (max - min) + min).toInt()
    }

    // Convert hex string to ByteArray
    fun hexToBinary(hex: String): ByteArray {
        require(hex.length == 64) { "tx_hash must be 64 characters (32 bytes in hex)" }
        return DatatypeConverter.parseHexBinary(hex)
    }

    // Convert ByteArray to hex string
    fun binaryToHex(binary: ByteArray): String {
        require(binary.size == 32) { "tx_hash must be 32 bytes" }
        return DatatypeConverter.printHexBinary(binary).lowercase()
    }
}