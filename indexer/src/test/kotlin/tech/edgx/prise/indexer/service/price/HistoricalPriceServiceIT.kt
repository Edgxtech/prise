package tech.edgx.prise.indexer.service.price

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.mockk.every
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.core.parameter.parametersOf
import org.koin.test.inject
import org.koin.test.mock.declareMock
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.domain.LatestCandlesView
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.repository.*
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.Base
import tech.edgx.prise.indexer.service.CandleService
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.math.BigInteger
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.stream.Stream

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HistoricalPriceServiceIT: Base() {

    val historicalPriceService: HistoricalPriceService by inject { parametersOf(config) }
    val candleService: CandleService by inject { parametersOf(config) }
//    val baseCandleRepository: BaseCandleRepository by inject { parametersOf(config.appDataSource) }
    val baseCandleRepository: BaseCandleRepository by inject { parametersOf(config.appDatabase) }
    val latestPriceService: LatestPriceService by inject { parametersOf(config) }

    @Test
    fun testMakeKnownFifteenCandle() {
        baseCandleRepository.truncateAllCandles()

        // Swaps: where slot >= 118544409 and slot < 118545309
        val swaps = listOf(
            Swap("37e3a61df516df10e982e002a8edb34f30b0e7874a5806bb6466ab23457e0674",118544843L,1,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", BigInteger.valueOf(3044755254),BigInteger.valueOf(8763634253),1),
            Swap("5614ee00eb985516967d7b6668d73a9534eb6dd1753b97f97a937a9b647dfb1a",118545044L,2,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e",BigInteger.valueOf(1454121764),BigInteger.valueOf(4165587265),0),
            Swap("a4b8224f1926c3663dc7b9df7818ba60c3a0661893df899ad577e9ffacb27520",118545170L,1,"lovelace","1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e",BigInteger.valueOf(1900760542),BigInteger.valueOf(5441037479),0)
        )
        // > Made candles: [CandleDTO(symbol=1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e, time=1710110700, open=0.34743066245122084, high=0.34933788810242455, low=0.34743066245122084, close=0.34933788810242455, volume=6399.63756)]

        val candleDtg = Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), Helpers.convertSlotToDtg(swaps[1].slot))

        val symbolsMade = historicalPriceService.makeCandlesFromSwaps(candleDtg, Helpers.smallestDuration, swaps)
        println("Made candles for: $symbolsMade")

        val candles = candleService.getFifteenById(swaps[0].asset2Unit) //, from-1000, to+1000
            .map { CandleDTO(it.symbol, it.time, it.open, it.high, it.low, it.close, it.volume) }
        println("Made candles: $candles")
    }

    @Test
    fun batchProcessHistoricalPrices_TriggerUpdate() {
        baseCandleRepository.truncateAllCandles()

        declareMock<AssetService> {
            every { getAssetByUnit("unitb") } returns Asset.invoke { unit = "unitb"; native_name = "assetb"; decimals = 6 }
            every { getAssetByUnit("unitc") } returns Asset.invoke { unit = "unitc"; native_name = "assetc"; decimals = 6 }
            every { getAssetByUnit("lovelace") } returns Asset.invoke { unit = "lovelace"; native_name = "ADA"; decimals = 6 }
            every { countAssets() } returns 2
        }
        val mockedAssetService: AssetService by inject()
        val historicalPriceServiceWithMocks: HistoricalPriceService by inject { parametersOf(config) }
        /* shouldnt have to override this, not sure why the mock isn't detected automatically */
        historicalPriceServiceWithMocks.assetService = mockedAssetService
        historicalPriceServiceWithMocks.CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SWAPS = 0

        historicalPriceServiceWithMocks.initialiseLastCandleState(
            Helpers.allResoDurations.map {
                it to Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), Helpers.convertSlotToDtg(100L)).toEpochSecond(Helpers.zoneOffset)
            }.toMap()
        )
        historicalPriceServiceWithMocks.bufferedSwaps.clear()
        val slot = 1000L
        val swaps = listOf(
            Swap("hash1", slot, 0, "unita", "unitb", BigInteger.valueOf(1000), BigInteger.valueOf(5000), 0),
            Swap("hash2", slot, 0, "unita", "unitc", BigInteger.valueOf(2000), BigInteger.valueOf(6000), 1)
        )
        println("Queued swaps: ${historicalPriceServiceWithMocks.bufferedSwaps}")
        assertTrue(historicalPriceServiceWithMocks.bufferedSwaps.isEmpty())
        /* Necessary to avoid triggering candle make */
        historicalPriceServiceWithMocks.previousCandleDtgState[Duration.ofMinutes(15)]=Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), Helpers.convertSlotToDtg(slot))
        historicalPriceServiceWithMocks.batchProcessHistoricalPrices(swaps, slot, false)
        println("Queued swaps: ${historicalPriceService.bufferedSwaps}")
        assertTrue(historicalPriceServiceWithMocks.bufferedSwaps.size==2)
        historicalPriceServiceWithMocks.bufferedSwaps.sortedBy { it.asset2Unit }.zip(swaps).forEach {
            assertTrue(it.first.txHash == it.second.txHash)
            assertTrue(it.first.slot == it.second.slot)
            assertTrue(it.first.asset2Unit == it.second.asset2Unit)
            assertTrue(it.first.amount1 == it.second.amount1)
            assertTrue(it.first.amount2 == it.second.amount2)
            assertTrue(it.first.operation == it.second.operation)
        }
        assertTrue(historicalPriceServiceWithMocks.bufferedSwaps[0].asset2Unit=="unitb")
        assertTrue(historicalPriceServiceWithMocks.bufferedSwaps[1].asset2Unit=="unitc")
        val unitBCandles = candleService.getFifteenByIdFromTo("unitb", slot - 100 - Helpers.slotConversionOffset, slot + 100 - Helpers.slotConversionOffset)
        val unitCCandles = candleService.getFifteenByIdFromTo("unitc", slot - 100 - Helpers.slotConversionOffset, slot + 100 - Helpers.slotConversionOffset)
        println("unitb candles: $unitBCandles")
        assertTrue(unitBCandles.isNotEmpty())
        assertTrue(unitBCandles.first().time == 1591567200L)
        assertTrue(unitBCandles.first().open == 0.2)
        assertTrue(unitBCandles.first().high == 0.2)
        assertTrue(unitBCandles.first().low == 0.2)
        assertTrue(unitBCandles.first().close == 0.2)
        assertTrue(unitBCandles.first().volume == 0.001)
        assertTrue(unitCCandles.isNotEmpty())
    }

    @Test
    fun batchProcessHistoricalPrices_TriggersFinaliseAndInitialise() {
        baseCandleRepository.truncateAllCandles()

        declareMock<AssetService> {
            every { getAssetByUnit("unitb") } returns Asset.invoke { unit = "unitb"; native_name = "assetb"; decimals = 6 }
            //every { getAssetByUnit("unitc") } returns Asset.invoke { unit = "unitc"; native_name = "assetc"; decimals = 6 }
            every { getAssetByUnit("lovelace") } returns Asset.invoke { unit = "lovelace"; native_name = "ADA"; decimals = 6 }
            every { countAssets() } returns 2
        }
        val mockedAssetService: AssetService by inject()
        val historicalPriceServiceWithMocks: HistoricalPriceService by inject { parametersOf(config) }
        /* shouldnt have to override this, not sure why the mock isn't detected automatically */
        historicalPriceServiceWithMocks.assetService = mockedAssetService
        historicalPriceServiceWithMocks.CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SWAPS = 0

        /* First call - should only trigger an update candle make */
        historicalPriceServiceWithMocks.initialiseLastCandleState(
            Helpers.allResoDurations.map {
                it to Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), Helpers.convertSlotToDtg(100L)).toEpochSecond(Helpers.zoneOffset)
            }.toMap()
        )
        historicalPriceServiceWithMocks.bufferedSwaps.clear()
        var slot = 1000L
        var swaps = listOf(
            Swap("hash1", slot, 0, "unita", "unitb", BigInteger.valueOf(1000), BigInteger.valueOf(5000), 0),
        )
        /* Necessary to avoid triggering candle make */
        historicalPriceServiceWithMocks.previousCandleDtgState[Duration.ofMinutes(15)]=Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), Helpers.convertSlotToDtg(slot))
        historicalPriceServiceWithMocks.batchProcessHistoricalPrices(swaps, slot, false)

        /* Second call, simulating after 15min (900seconds) has elapsed - should trigger a finalise and initialise candle make */
        slot = 1000L + 901L
        swaps = listOf(
            Swap("hash3", slot, 0, "unita", "unitb", BigInteger.valueOf(1000), BigInteger.valueOf(5100), 0),
            Swap("hash4", slot, 0, "unita", "unitb", BigInteger.valueOf(1000), BigInteger.valueOf(5050), 0),
        )
        historicalPriceServiceWithMocks.batchProcessHistoricalPrices(swaps, slot, false)
        val unitBCandles = candleService.getFifteenByIdFromTo("unitb", slot - 1000 - Helpers.slotConversionOffset, slot + 1000 - Helpers.slotConversionOffset)
        println("unitb candles: $unitBCandles")
        assertTrue(unitBCandles.isNotEmpty())
        assertTrue(unitBCandles.first().time == 1591567200L)
        assertTrue(unitBCandles.first().open == 0.2)
        assertTrue(unitBCandles.first().high == 0.2)
        assertTrue(unitBCandles.first().low == 0.2)
        assertTrue(unitBCandles.first().close == 0.2)
        assertTrue(unitBCandles.first().volume == 0.001)
        assertTrue(unitBCandles[1].time == 1591568100L)
        assertTrue(unitBCandles[1].open == 0.2)
        assertTrue(unitBCandles[1].high == 0.2)
        assertTrue(unitBCandles[1].low == 0.19607843137254902)
        assertTrue(unitBCandles[1].close == 0.19607843137254902)
        assertTrue(unitBCandles[1].volume == 0.002)
    }

    @Test
    fun makeCandlesFromSwaps_10tx() {
        baseCandleRepository.truncateAllCandles()
        val candlesMap = testMakeAndReturnCandlesFromSwaps("src/test/resources/testdata/wingriders/swaps_from_block_9896194_10tx.json", true)
        val knownCandles: Map<String,List<CandleDTO>> = Gson().fromJson(
            File("src/test/resources/testdata/wingriders/candles_from_block_9896194_10tx.json")
                .readText(Charsets.UTF_8)
                .byteInputStream()
                .bufferedReader().readLine(),
            object : TypeToken<HashMap<String,List<CandleDTO>>>() {}.type)
        candlesMap.forEach { it ->
            val assetCandles: List<CandleDTO> = knownCandles[it.key]!!
            it.value.zip(assetCandles).forEach { candlePair ->
                println("Comparing candles: $candlePair")
                assertTrue(candlePair.first.time == candlePair.second.time)
                assertTrue(candlePair.first.symbol == candlePair.second.symbol)
                assertTrue(candlePair.first.open == candlePair.second.open)
                assertTrue(candlePair.first.high == candlePair.second.high)
                assertTrue(candlePair.first.low == candlePair.second.low)
                assertTrue(candlePair.first.close == candlePair.second.close)
                assertTrue(candlePair.first.volume == candlePair.second.volume)
            }
        }
    }

    /* Helper for testing. Makes, persists and returns candles */
    fun testMakeAndReturnCandlesFromSwaps(file: String, addContinuationCandlesAtEachStep: Boolean): Map<String,List<CandleDTO>> {
        val knownSwaps: List<Swap> = Gson().fromJson(
            File(file).readText(Charsets.UTF_8).byteInputStream().bufferedReader().readLine(),
            object : TypeToken<ArrayList<Swap>>() {}.type)
        println("Swaps: ${knownSwaps}")
        /* Must be treemap to keep the dtg key ordered */
        val groupedSwaps = TreeMap(knownSwaps.groupBy {
            Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), Helpers.convertSlotToDtg(it.slot))
        }.toMutableMap())
        println("Grouped swaps: $groupedSwaps")

        // pad it out with missing dtg's
        val dtr = DateTimeRange(
            groupedSwaps.keys.min(),
            groupedSwaps.keys.max(),
            Duration.ofMinutes(15).toMinutesPart()
        )
        dtr.forEach {
            if(!groupedSwaps.keys.contains(it)) {
                println("Adding missing dtg group: $it, ${it.toEpochSecond(Helpers.zoneOffset)}")
                groupedSwaps[it] = listOf()
            }
        }
        val assetIds = knownSwaps.map { it.asset2Unit }
        println("AssetIds: $assetIds")
        groupedSwaps.forEach {
            /* For testing small amounts of swaps/candles, need to skip batching */
            historicalPriceService.CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SWAPS = 0
            /* For testing need to run swaps through latestPriceService to make asset definitions */
            it.value.forEach {
                swap -> latestPriceService.batchProcessLatestPrices(swap)
            }
            val symbolsMade = historicalPriceService.makeCandlesFromSwaps(it.key, Helpers.smallestDuration, it.value)
            println("Made candles for: $symbolsMade")

            if (addContinuationCandlesAtEachStep) {
                // Alternate: historicalPriceService.populateContinuationCandles(candleDtg = it.key, Duration.ofMinutes(15), listOf())
                val continuationCandleData = candleService.getContinuationCandleData(candleDtg = it.key, Duration.ofMinutes(15), listOf())
                candleService.batchPersistOrUpdate(continuationCandleData, Duration.ofMinutes(15))
            }
        }
        /* Retrieve newly made candles */
        val allCandles: Map<String,List<CandleDTO>> = assetIds
            .map { symbol -> symbol to
                    candleService.getFifteenById(symbol) //, from-1000, to+1000
                        .map { CandleDTO(it.symbol, it.time, it.open, it.high, it.low, it.close, it.volume) } }
            .toMap()
        return allCandles
    }

    /* Helper for testing. Makes, persists and returns candles */
    fun testMakeAndReturnCandlesFromSubCandles(swapsFile: String): Map<String,List<CandleDTO>> {
        val candlesMap15m = testMakeAndReturnCandlesFromSwaps(swapsFile, true)
        println("Candlesmap 15m: $candlesMap15m")
        /* pre-known */
        val knownHourlyCandleTime = 1707181200L
        val hourlyCandleDtg = LocalDateTime.ofEpochSecond(knownHourlyCandleTime, 0, Helpers.zoneOffset)
        val assetIds = candlesMap15m.map { it.key }
        historicalPriceService.CANDLE_PERSISTANCE_BATCH_SIZE_FROM_SUBS = 0
        historicalPriceService.makeCandlesFromSubCandles(hourlyCandleDtg, Duration.ofHours(1))
        /* Retrieve newly made candles */
        val allCandles: Map<String,List<CandleDTO>> = assetIds
            .map { symbol -> symbol to
                    candleService.getHourlyById(symbol) //, knownHourlyCandleTime-1000, knownHourlyCandleTime+1000)
                        .map { CandleDTO(it.symbol, it.time, it.open, it.high, it.low, it.close, it.volume) } }
            .toMap()
        return allCandles
    }

    internal class DateTimeRange(
        private val startDateTime: LocalDateTime,
        private val endDateTime: LocalDateTime,
        private val resolution: Int) : Iterable<LocalDateTime> {
        override fun iterator(): MutableIterator<LocalDateTime> {
            return stream().iterator()
        }
        fun stream(): Stream<LocalDateTime> {
            return Stream.iterate(startDateTime)
            { d: LocalDateTime -> d.plusMinutes(resolution.toLong()) }
                .limit(ChronoUnit.MINUTES.between(startDateTime, endDateTime) / resolution + 1)
        }
    }
}