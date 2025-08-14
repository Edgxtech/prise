//package tech.edgx.prise.indexer.service
//
//import kotlinx.coroutines.runBlocking
//import org.junit.Ignore
//import org.junit.jupiter.api.Assertions.assertTrue
//import org.junit.jupiter.api.BeforeAll
//import org.junit.jupiter.api.Test
//import org.koin.core.parameter.parametersOf
//import org.koin.test.inject
//import tech.edgx.prise.indexer.Base
//import tech.edgx.prise.indexer.domain.Asset
//import tech.edgx.prise.indexer.model.DexEnum
//import tech.edgx.prise.indexer.model.prices.CandleDTO
//import tech.edgx.prise.indexer.repository.*
//import tech.edgx.prise.indexer.util.Helpers
//import java.time.Duration
//import java.time.LocalDateTime
//
//class CandleServiceIT: Base() {
//
//    val candleService: CandleService by inject { parametersOf(config) }
//    val baseCandleRepository : BaseCandleRepository by inject{ parametersOf(config.appDatabase) }
//    val assetService: AssetService by inject { parametersOf(config) }
//    lateinit var testAsset: Asset
//
//    @BeforeAll
//    fun initialiseAsset() {
//        val testUnit = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459"
//        assetService.insertOrUpdate(Asset.invoke{
//            unit=testUnit
//            pricing_provider=DexEnum.WINGRIDERS.nativeName
//        })
//        testAsset = assetService.getAssetByUnit(testUnit)!!
//    }
//
//    @Ignore
//    @Test
//    fun clearAllCandles() {
//        baseCandleRepository.truncateAllCandles()
//    }
//
//    @Test
//    fun crudTest() {
//        baseCandleRepository.truncateAllCandles()
//        val testCandleDate = Helpers.toNearestDiscreteDate(Duration.ofDays(7), LocalDateTime.now().minusWeeks(2)) // Aligned to a nearest week, so it tests correctly for all reso's
//        val testWeeklyCandle = CandleDTO(
//            testAsset.id,
//            testAsset.unit,
//            testCandleDate.toEpochSecond(Helpers.zoneOffset),
//            20.0,
//            20.0,
//            20.0,
//            20.0,
//            2000.0)
//        val testDailyCandle = CandleDTO(
//            testAsset.id,
//            testAsset.unit,
//            testCandleDate.toEpochSecond(Helpers.zoneOffset),
//            15.0,
//            15.0,
//            15.0,
//            15.0,
//            1500.0)
//        val testHourlyCandle = CandleDTO(
//            testAsset.id,
//            testAsset.unit,
//            testCandleDate.toEpochSecond(Helpers.zoneOffset),
//            10.0,
//            10.0,
//            10.0,
//            10.0,
//            1000.0)
//        val testFifteenCandle = CandleDTO(
//            testAsset.id,
//            testAsset.unit,
//            testCandleDate.toEpochSecond(Helpers.zoneOffset),
//            9.0,
//            11.0,
//            8.0,
//            10.0,
//            9000.0)
//        runBlocking {
//            candleService.persistOrUpdate(listOf(testWeeklyCandle), Duration.ofDays(7))
//            candleService.persistOrUpdate(listOf(testDailyCandle), Duration.ofDays(1))
//            candleService.persistOrUpdate(listOf(testHourlyCandle), Duration.ofHours(1))
//            candleService.persistOrUpdate(listOf(testFifteenCandle), Duration.ofMinutes(15))
//        }
//        val lastCandles = candleService.getLastCandleForMultiple()
//        assertTrue(lastCandles.size==4)
//        val latestWeeklyCandle = lastCandles.filter { it.resolution == Helpers.RESO_DEFN_1W }.first().let { CandleDTO(testAsset.id, testAsset.unit, it.time, it.open, it.high, it.low, it.close, it.volume) }
//        assertTrue(latestWeeklyCandle == testWeeklyCandle)
//        val latestDailyCandle = lastCandles.filter { it.resolution == Helpers.RESO_DEFN_1D }.first().let { CandleDTO(testAsset.id, testAsset.unit, it.time, it.open, it.high, it.low, it.close, it.volume) }
//        assertTrue(latestDailyCandle == testDailyCandle)
//        val latestHourlyCandle = lastCandles.filter { it.resolution == Helpers.RESO_DEFN_1H }.first().let { CandleDTO(testAsset.id, testAsset.unit, it.time, it.open, it.high, it.low, it.close, it.volume) }
//        assertTrue(latestHourlyCandle == testHourlyCandle)
//        val latestFifteenCandle = lastCandles.filter { it.resolution == Helpers.RESO_DEFN_15M }.first().let { CandleDTO(testAsset.id, testAsset.unit, it.time, it.open, it.high, it.low, it.close, it.volume) }
//        assertTrue(latestFifteenCandle == testFifteenCandle)
//        baseCandleRepository.truncateAllCandles()
//    }
//
//    @Test
//    fun getCandlesBetweenTimes() {
//        baseCandleRepository.truncateAllCandles()
//        val testUnit = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459"
//        val testCandleDate = Helpers.toNearestDiscreteDate(Duration.ofDays(7), LocalDateTime.now().minusWeeks(1))
//        val testFifteenCandle = CandleDTO(
//            testAsset.id,
//            testAsset.unit,
//            testCandleDate.toEpochSecond(Helpers.zoneOffset),
//            9.0,
//            11.0,
//            8.0,
//            10.0,
//            9000.0)
//        candleService.persistOrUpdate(listOf(testFifteenCandle), Duration.ofMinutes(15))
//        val fromTime = testCandleDate.toEpochSecond(Helpers.zoneOffset)
//        val candles = candleService.getNCandlesBetweenTimes(Duration.ofMinutes(15), fromTime, fromTime + 3600)
//        println("Retrieved candles: ${candles.size}")
//        assertTrue(candles.size==1)
//    }
//
//    @Test
//    fun checkAndMakeIndexes() {
//        baseCandleRepository.addIndexesIfRequired()
//    }
//}