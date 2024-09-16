package tech.edgx.prise.indexer.util

import com.bloxbean.cardano.client.crypto.Bech32
import com.bloxbean.cardano.client.util.HexUtil
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.domain.LatestCandlesView
import tech.edgx.prise.indexer.model.DexEnum
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.service.classifier.module.*
import java.math.BigInteger
import java.time.*
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters
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

    val dexLaunchAdjustedBlockHash: String = "aa937b19ea01809f00c51994f21ca02ccda03618806c265d9c605d4834c07595"
    val dexLaunchAdjustedBlockSlot: Long = 50400919L

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

    fun getDexName(dexNumber: Int): String {
        when (dexNumber) {
            0 -> { return DexEnum.WINGRIDERS.nativeName }
            1 -> { return DexEnum.SUNDAESWAP.nativeName }
            2 -> { return DexEnum.MINSWAP.nativeName }
            3 -> { return DexEnum.MINSWAPV2.nativeName}
        }
        throw Exception("No mapping for dex number $dexNumber")
    }

    fun getDexNumber(dexName: String): Int {
        when (dexName) {
            DexEnum.WINGRIDERS.nativeName -> { return 0 }
            DexEnum.SUNDAESWAP.nativeName -> { return 1 }
            DexEnum.MINSWAP.nativeName -> { return 2 }
            DexEnum.MINSWAPV2.nativeName -> { return 3 }
        }
        throw Exception("No mapping for dex name $dexName")
    }

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

    fun convertCandleViewToCandleDTO(candle: LatestCandlesView?): CandleDTO? {
        return candle?.let {
            CandleDTO(
                it.symbol,
                it.time,
                it.open,
                it.high,
                it.low,
                it.close,
                it.volume
            )
        }
    }

    fun getRandomNumber(min: Int, max: Int): Int {
        return (Math.random() * (max - min) + min).toInt()
    }
}