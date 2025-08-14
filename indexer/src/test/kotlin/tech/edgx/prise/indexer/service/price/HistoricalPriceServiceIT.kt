//package tech.edgx.prise.indexer.service.price
//
//import com.google.gson.Gson
//import com.google.gson.reflect.TypeToken
//import io.mockk.every
//import org.junit.jupiter.api.Assertions.assertEquals
//import org.junit.jupiter.api.Assertions.assertTrue
//import org.junit.jupiter.api.Test
//import org.junit.jupiter.api.TestInstance
//import org.koin.core.parameter.parametersOf
//import org.koin.test.inject
//import org.koin.test.mock.declareMock
//import tech.edgx.prise.indexer.domain.Asset
//import tech.edgx.prise.indexer.domain.Price
//import tech.edgx.prise.indexer.model.prices.CandleDTO
//import tech.edgx.prise.indexer.repository.BaseCandleRepository
//import tech.edgx.prise.indexer.service.AssetService
//import tech.edgx.prise.indexer.Base
//import tech.edgx.prise.indexer.service.CandleService
//import tech.edgx.prise.indexer.util.Helpers
//import java.io.File
//import java.math.BigDecimal
//import java.time.Duration
//import java.time.LocalDateTime
//import java.time.temporal.ChronoUnit
//import java.util.*
//import java.util.stream.Stream
//
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//class HistoricalPriceServiceIT : Base() {
//
//    val historicalPriceService: HistoricalPriceService by inject { parametersOf(config) }
//    val candleService: CandleService by inject { parametersOf(config) }
//    val baseCandleRepository: BaseCandleRepository by inject { parametersOf(config.appDatabase) }
//    val latestPriceService: LatestPriceService by inject { parametersOf(config) }
//
//    @Test
//    fun testMakeKnownFifteenCandle() {
//        baseCandleRepository.truncateAllCandles()
//
//        // Prices: where dtg >= 118544409 and dtg < 118545309
//        val prices = listOf(
//            Price {
//                provider = 1
//                asset_id = 1L
//                quote_asset_id = 2L
//                price = 0.34743066245122084
//                dtg = 118544843L
//                tx_hash = "37e3a61df516df10e982e002a8edb34f30b0e7874a5806bb6466ab23457e0674"
//                amount1 = BigDecimal("3044755254")
//                amount2 = BigDecimal("8763634253")
//                operation = 1
//            },
//            Price {
//                provider = 2
//                asset_id = 1L
//                quote_asset_id = 2L
//                price = 0.34933788810242455
//                dtg = 118545044L
//                tx_hash = "5614ee00eb985516967d7b6668d73a9534eb6dd1753b97f97a937a9b647dfb1a"
//                amount1 = BigDecimal("1454121764")
//                amount2 = BigDecimal("4165587265")
//                operation = 0
//            },
//            Price {
//                provider = 1
//                asset_id = 1L
//                quote_asset_id = 2L
//                price = 0.34933788810242455
//                dtg = 118545170L
//                tx_hash = "a4b8224f1926c3663dc7b9df7818ba60c3a0661893df899ad577e9ffacb27520"
//                amount1 = BigDecimal("1900760542")
//                amount2 = BigDecimal("5441037479")
//                operation = 0
//            }
//        )
//
//        val candleDtg = Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.ofEpochSecond(prices[1].dtg, 0, Helpers.zoneOffset))
//
//        val candlesMade = historicalPriceService.makeCandlesFromPrices(candleDtg, Helpers.smallestDuration, prices)
//        println("Made candles for: $candlesMade")
//
//        val candles = candleService.getFifteenById(prices[0].quote_asset_id.toString())
//            .map { CandleDTO(it.asset_id, "", it.time, it.open, it.high, it.low, it.close, it.volume) }
//        println("Made candles: $candles")
//
//        assertTrue(candles.isNotEmpty())
//        val candle = candles.find { it.time == 1710110700L }
//        assertTrue(candle != null)
//        assertEquals(0.34743066245122084, candle?.open, 1e-10)
//        assertEquals(0.34933788810242455, candle?.high, 1e-10)
//        assertEquals(0.34743066245122084, candle?.low, 1e-10)
//        assertEquals(0.34933788810242455, candle?.close, 1e-10)
//        assertEquals(6399.63756, candle?.volume, 1e-10)
//    }
//
//    @Test
//    fun batchProcessHistoricalPrices_TriggerUpdate() {
//        baseCandleRepository.truncateAllCandles()
//
//        declareMock<AssetService> {
//            every { getAssetByUnit("unitb") } returns Asset.invoke { id = 2L; unit = "unitb"; native_name = "assetb"; decimals = 6 }
//            every { getAssetByUnit("unitc") } returns Asset.invoke { id = 3L; unit = "unitc"; native_name = "assetc"; decimals = 6 }
//            every { getAssetByUnit("lovelace") } returns Asset.invoke { id = 1L; unit = "lovelace"; native_name = "ADA"; decimals = 6 }
//            every { countAssets() } returns 3
//            every { getAssetsByUnits(setOf("unitb", "unitc", "lovelace")) } returns mapOf(
//                "unitb" to Asset.invoke { id = 2L; unit = "unitb"; native_name = "assetb"; decimals = 6 },
//                "unitc" to Asset.invoke { id = 3L; unit = "unitc"; native_name = "assetc"; decimals = 6 },
//                "lovelace" to Asset.invoke { id = 1L; unit = "lovelace"; native_name = "ADA"; decimals = 6 }
//            )
//        }
//        val mockedAssetService: AssetService by inject()
//        val historicalPriceServiceWithMocks: HistoricalPriceService by inject { parametersOf(config) }
//        historicalPriceServiceWithMocks.assetService = mockedAssetService
//        historicalPriceServiceWithMocks.CANDLE_PERSISTENCE_BATCH_SIZE = 0
//
//        historicalPriceServiceWithMocks.initialiseLastCandleState(
//            Helpers.allResoDurations.associateWith {
//                Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.ofEpochSecond(100L, 0, Helpers.zoneOffset)).toEpochSecond(Helpers.zoneOffset)
//            }
//        )
//        historicalPriceServiceWithMocks.bufferedPrices.clear()
//        val slot = 1000L
//        val prices = listOf(
//            Price {
//                provider = 0
//                asset_id = 1L
//                quote_asset_id = 2L
//                price = 0.2
//                dtg = slot
//                tx_hash = "hash1"
//                amount1 = BigDecimal("1000")
//                amount2 = BigDecimal("5000")
//                operation = 0
//            },
//            Price {
//                provider = 0
//                asset_id = 1L
//                quote_asset_id = 3L
//                price = 0.3333333333333333
//                dtg = slot
//                tx_hash = "hash2"
//                amount1 = BigDecimal("2000")
//                amount2 = BigDecimal("6000")
//                operation = 1
//            }
//        )
//        println("Queued prices: ${historicalPriceServiceWithMocks.bufferedPrices}")
//        assertTrue(historicalPriceServiceWithMocks.bufferedPrices.isEmpty())
//        historicalPriceServiceWithMocks.previousCandleDtgState[Duration.ofMinutes(15)] = Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.ofEpochSecond(slot, 0, Helpers.zoneOffset))
//        historicalPriceServiceWithMocks.processHistoricalPrices(prices, slot, false)
//        println("Queued prices: ${historicalPriceServiceWithMocks.bufferedPrices}")
//        assertEquals(2, historicalPriceServiceWithMocks.bufferedPrices.size)
//        historicalPriceServiceWithMocks.bufferedPrices.sortedBy { it.quote_asset_id }.zip(prices).forEach {
//            assertEquals(it.first.tx_hash, it.second.tx_hash)
//            assertEquals(it.first.dtg, it.second.dtg)
//            assertEquals(it.first.quote_asset_id, it.second.quote_asset_id)
//            assertEquals(it.first.amount1, it.second.amount1)
//            assertEquals(it.first.amount2, it.second.amount2)
//            assertEquals(it.first.operation, it.second.operation)
//        }
//        assertEquals(2L, historicalPriceServiceWithMocks.bufferedPrices[0].quote_asset_id)
//        assertEquals(3L, historicalPriceServiceWithMocks.bufferedPrices[1].quote_asset_id)
//        val unitBCandles = candleService.getFifteenByIdFromTo("unitb", slot - 100, slot + 100)
//        val unitCCandles = candleService.getFifteenByIdFromTo("unitc", slot - 100, slot + 100)
//        println("unitb candles: $unitBCandles")
//        assertTrue(unitBCandles?.isNotEmpty() == true)
//        assertEquals(1591567200L, unitBCandles.first().time)
//        assertEquals(0.2, unitBCandles.first().open, 1e-10)
//        assertEquals(0.2, unitBCandles.first().high, 1e-10)
//        assertEquals(0.2, unitBCandles.first().low, 1e-10)
//        assertEquals(0.2, unitBCandles.first().close, 1e-10)
//        assertEquals(0.001, unitBCandles.first().volume, 1e-10)
//        assertTrue(unitCCandles?.isNotEmpty() == true)
//    }
//
//    @Test
//    fun batchProcessHistoricalPrices_TriggersFinaliseAndInitialise() {
//        baseCandleRepository.truncateAllCandles()
//
//        declareMock<AssetService> {
//            every { getAssetByUnit("unitb") } returns Asset.invoke { id = 2L; unit = "unitb"; native_name = "assetb"; decimals = 6 }
//            every { getAssetByUnit("lovelace") } returns Asset.invoke { id = 1L; unit = "lovelace"; native_name = "ADA"; decimals = 6 }
//            every { countAssets() } returns 2
//            every { getAssetsByUnits(setOf("unitb", "lovelace")) } returns mapOf(
//                "unitb" to Asset.invoke { id = 2L; unit = "unitb"; native_name = "assetb"; decimals = 6 },
//                "lovelace" to Asset.invoke { id = 1L; unit = "lovelace"; native_name = "ADA"; decimals = 6 }
//            )
//        }
//        val mockedAssetService: AssetService by inject()
//        val historicalPriceServiceWithMocks: HistoricalPriceService by inject { parametersOf(config) }
//        historicalPriceServiceWithMocks.assetService = mockedAssetService
//        historicalPriceServiceWithMocks.CANDLE_PERSISTENCE_BATCH_SIZE = 0
//
//        historicalPriceServiceWithMocks.initialiseLastCandleState(
//            Helpers.allResoDurations.associateWith {
//                Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.ofEpochSecond(100L, 0, Helpers.zoneOffset)).toEpochSecond(Helpers.zoneOffset)
//            }
//        )
//        historicalPriceServiceWithMocks.bufferedPrices.clear()
//        var slot = 1000L
//        var prices = listOf(
//            Price {
//                provider = 0
//                asset_id = 1L
//                quote_asset_id = 2L
//                price = 0.2
//                dtg = slot
//                tx_hash = "hash1"
//                amount1 = BigDecimal("1000")
//                amount2 = BigDecimal("5000")
//                operation = 0
//            }
//        )
//        historicalPriceServiceWithMocks.previousCandleDtgState[Duration.ofMinutes(15)] = Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.ofEpochSecond(slot, 0, Helpers.zoneOffset))
//        historicalPriceServiceWithMocks.processHistoricalPrices(prices, slot, false)
//
//        slot += 901L
//        prices = listOf(
//            Price {
//                provider = 0
//                asset_id = 1L
//                quote_asset_id = 2L
//                price = 0.19607843137254902
//                dtg = slot
//                tx_hash = "hash3"
//                amount1 = BigDecimal("1000")
//                amount2 = BigDecimal("5100")
//                operation = 0
//            },
//            Price {
//                provider = 0
//                asset_id = 1L
//                quote_asset_id = 2L
//                price = 0.19801980198019803
//                dtg = slot
//                tx_hash = "hash4"
//                amount1 = BigDecimal("1000")
//                amount2 = BigDecimal("5050")
//                operation = 0
//            }
//        )
//        historicalPriceServiceWithMocks.processHistoricalPrices(prices, slot, false)
//        val unitBCandles = candleService.getFifteenByIdFromTo("unitb", slot - 1000, slot + 1000)
//        println("unitb candles: $unitBCandles")
//        assertTrue(unitBCandles?.isNotEmpty() == true)
//        assertEquals(1591567200L, unitBCandles.first().time)
//        assertEquals(0.2, unitBCandles.first().open, 1e-10)
//        assertEquals(0.2, unitBCandles.first().high, 1e-10)
//        assertEquals(0.2, unitBCandles.first().low, 1e-10)
//        assertEquals(0.2, unitBCandles.first().close, 1e-10)
//        assertEquals(0.001, unitBCandles.first().volume, 1e-10)
//        assertEquals(1591568100L, unitBCandles[1].time)
//        assertEquals(0.2, unitBCandles[1].open, 1e-10)
//        assertEquals(0.2, unitBCandles[1].high, 1e-10)
//        assertEquals(0.19607843137254902, unitBCandles[1].low, 1e-10)
//        assertEquals(0.19801980198019803, unitBCandles[1].close, 1e-10)
//        assertEquals(0.002, unitBCandles[1].volume, 1e-10)
//    }
//
//    @Test
//    fun makeCandlesFromPrices_10tx() {
//        baseCandleRepository.truncateAllCandles()
//        val candlesMap = testMakeAndReturnCandlesFromPrices("src/test/resources/testdata/wingriders/swaps_from_block_9896194_10tx.json", true)
//        val knownCandles: Map<String, List<CandleDTO>> = Gson().fromJson(
//            File("src/test/resources/testdata/wingriders/candles_from_block_9896194_10tx.json")
//                .readText(Charsets.UTF_8)
//                .byteInputStream()
//                .bufferedReader().readLine(),
//            object : TypeToken<HashMap<String, List<CandleDTO>>>() {}.type
//        )
//        candlesMap.forEach { (symbol, candles) ->
//            val assetCandles: List<CandleDTO> = knownCandles[symbol]!!
//            candles.zip(assetCandles).forEach { candlePair ->
//                println("Comparing candles: $candlePair")
//                assertEquals(candlePair.first.time, candlePair.second.time)
//                assertEquals(candlePair.first.symbol, candlePair.second.symbol)
//                assertEquals(candlePair.first.open, candlePair.second.open, 1e-10)
//                assertEquals(candlePair.first.high, candlePair.second.high, 1e-10)
//                assertEquals(candlePair.first.low, candlePair.second.low, 1e-10)
//                assertEquals(candlePair.first.close, candlePair.second.close, 1e-10)
//                assertEquals(candlePair.first.volume, candlePair.second.volume, 1e-10)
//            }
//        }
//    }
//
//    fun testMakeAndReturnCandlesFromPrices(file: String, addContinuationCandlesAtEachStep: Boolean): Map<String, List<CandleDTO>> {
//        val knownPrices: List<Price> = Gson().fromJson(
//            File(file).readText(Charsets.UTF_8).byteInputStream().bufferedReader().readLine(),
//            object : TypeToken<ArrayList<Price>>() {}.type
//        ).map {
//            Price {
//                provider = it.provider
//                asset_id = it.asset_id
//                quote_asset_id = it.quote_asset_id
//                price = it.price
//                dtg = it.dtg
//                tx_hash = it.tx_hash
//                amount1 = it.amount1
//                amount2 = it.amount2
//                operation = it.operation
//            }
//        }
//        println("Prices: $knownPrices")
//        val groupedPrices = TreeMap(knownPrices.groupBy {
//            Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.ofEpochSecond(it.dtg, 0, Helpers.zoneOffset))
//        }.toMutableMap())
//        println("Grouped prices: $groupedPrices")
//
//        // Pad with missing dtg's
//        val dtr = DateTimeRange(
//            groupedPrices.keys.min(),
//            groupedPrices.keys.max(),
//            Duration.ofMinutes(15).toMinutesPart()
//        )
//        dtr.forEach {
//            if (!groupedPrices.keys.contains(it)) {
//                println("Adding missing dtg group: $it, ${it.toEpochSecond(Helpers.zoneOffset)}")
//                groupedPrices[it] = listOf()
//            }
//        }
//        val assetIds = knownPrices.map { it.quote_asset_id.toString() }.distinct()
//        println("AssetIds: $assetIds")
//        groupedPrices.forEach { (candleDtg, prices) ->
//            historicalPriceService.CANDLE_PERSISTENCE_BATCH_SIZE = 0
//            prices.forEach { price ->
//                latestPriceService.processPrice(price)
//            }
//            val candlesMade = historicalPriceService.makeCandlesFromPrices(candleDtg, Helpers.smallestDuration, prices)
//            println("Made candles for: $candlesMade")
//
//            if (addContinuationCandlesAtEachStep) {
//                val continuationCandleData = candleService.getContinuationCandleData(candleDtg, Duration.ofMinutes(15), listOf())
//                candleService.batchPersistOrUpdate(continuationCandleData, Duration.ofMinutes(15))
//            }
//        }
//        val allCandles: Map<String, List<CandleDTO>> = assetIds
//            .map { symbol ->
//                symbol to candleService.getFifteenById(symbol)
//                    .map { CandleDTO(it.asset_id, "", it.time, it.open, it.high, it.low, it.close, it.volume) }
//            }
//            .toMap()
//        return allCandles
//    }
//
//    internal class DateTimeRange(
//        private val startDateTime: LocalDateTime,
//        private val endDateTime: LocalDateTime,
//        private val resolution: Int
//    ) : Iterable<LocalDateTime> {
//        override fun iterator(): MutableIterator<LocalDateTime> {
//            return stream().iterator()
//        }
//        fun stream(): Stream<LocalDateTime> {
//            return Stream.iterate(startDateTime) { d: LocalDateTime -> d.plusMinutes(resolution.toLong()) }
//                .limit(ChronoUnit.MINUTES.between(startDateTime, endDateTime) / resolution + 1)
//        }
//    }
//}