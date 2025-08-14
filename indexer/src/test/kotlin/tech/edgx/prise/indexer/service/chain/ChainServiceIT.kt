//package tech.edgx.prise.indexer.service.chain
//
//import com.bloxbean.cardano.yaci.core.model.Block
//import com.google.gson.Gson
//import com.google.gson.GsonBuilder
//import com.google.gson.reflect.TypeToken
//import kotlinx.coroutines.*
//import org.junit.jupiter.api.Test
//import org.junit.jupiter.api.Assertions.*
//import org.junit.jupiter.api.TestInstance
//import org.koin.core.parameter.parametersOf
//import org.koin.test.inject
//import org.slf4j.LoggerFactory
//import tech.edgx.prise.indexer.Base
//import tech.edgx.prise.indexer.domain.Asset
//import tech.edgx.prise.indexer.model.FullyQualifiedTxDTO
//import tech.edgx.prise.indexer.model.prices.CandleDTO
//import tech.edgx.prise.indexer.repository.*
//import tech.edgx.prise.indexer.service.AssetService
//import tech.edgx.prise.indexer.service.CandleService
//import tech.edgx.prise.indexer.testutil.TransactionBodyExcludeStrategy
//import tech.edgx.prise.indexer.util.Helpers
//import java.io.File
//import java.time.Duration
//import java.time.LocalDateTime
//import java.util.*
//
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//class ChainServiceIT: Base() {
//    private val log = LoggerFactory.getLogger(javaClass)
//    val chainService: ChainService by inject { parametersOf(config) }
//    val baseCandleRepository: BaseCandleRepository by inject { parametersOf(config.appDatabase) }
//    val candleService: CandleService by inject { parametersOf(config) }
//    val assetService: AssetService by inject { parametersOf(config) }
//
//    val transactionBodyGson: Gson = GsonBuilder().addDeserializationExclusionStrategy(TransactionBodyExcludeStrategy()).create()
//
//    @Test
//    fun qualifyTransactions() {
//        val inputFile = "src/test/resources/testdata/blocks_from_9894905.json"
//        val blocks: List<Block> = transactionBodyGson.fromJson<List<Block>>(
//            File(inputFile).readText(Charsets.UTF_8).byteInputStream().bufferedReader().readLine(),
//            object : TypeToken<List<Block>>() {}.type)
//        val knownQualifiedTx: List<FullyQualifiedTxDTO> = Gson().fromJson(
//            File("src/test/resources/testdata/transactions_qualified_from_block_9894905.json")
//                .readText(Charsets.UTF_8)
//                .byteInputStream()
//                .bufferedReader().readLine(),
//            object : TypeToken<ArrayList<FullyQualifiedTxDTO>>() {}.type)
//        val computedQualifiedTx = blocks.flatMap {
//            chainService.qualifyTransactions(it.header.headerBody.slot, it.transactionBodies, it.transactionWitness)
//        }
//        println("QualifiedTxMap, #: ${computedQualifiedTx.size}")
//        var idx = 0
//        knownQualifiedTx.zip(computedQualifiedTx).forEach {
//            println("Comparing known tx, idx: $idx, ${it.first.witnesses} to computed tx: ${it.second.witnesses}")
//            println("Comparing known inputs: ${it.first.inputUtxos}")
//            assertTrue { it.first.txHash == it.second.txHash }
//            assertTrue { it.first.dexCode == it.second.dexCode }
//            assertTrue { it.first.dexCredential == it.second.dexCredential }
//            assertTrue { it.first.blockSlot == it.second.blockSlot }
//            // Temporary compare only # items, the amounts list is ordered differently for each Utxo Resolver preventing direct comparison
//            assertTrue { it.first.inputUtxos.size == it.second.inputUtxos.size }
//            assertTrue { it.first.outputUtxos == it.second.outputUtxos }
//            assertTrue { it.first.witnesses.toString() == it.second.witnesses.toString() }
//            idx++
//        }
//    }
//
//    @Test
//    fun autoSelectPoint_NoPriorCandles() {
//        baseCandleRepository.truncateAllCandles()
//        val time = candleService.getSyncPointTime()
//        println("Sync point time: $time")
//        val initState = chainService.determineInitialisationState(null)
//        println("Point auto selected: $initState")
//        assertTrue(initState.chainStartPoint.slot.equals(Helpers.dexLaunchAdjustedBlockSlot))
//        assertTrue(initState.chainStartPoint.hash.equals(Helpers.dexLaunchAdjustedBlockHash))
//    }
//
//    @Test
//    fun autoSelectPoint_ProvidedInConfig() {
//        val initState = chainService.determineInitialisationState(1707091220)
//        println("Point auto selected: $initState")
//        assertTrue(initState.chainStartPoint.slot.equals(115524906L))
//        assertTrue(initState.chainStartPoint.hash.equals("8352cd53032357dc0547cebc0ce7f280f86c63b1cbe656cedb8738451beff431"))
//    }
//
//    /*
//      The requirement here is to be able to continue where it left off, which will be the
//      point equal to the smallest reso duration table min of each symbol max time */
//    @Test
//    fun autoSelectPoint_PriorCandles_1() {
//        baseCandleRepository.truncateAllCandles()
//        val testUnit = "A"
//        assetService.insertOrUpdate( Asset.invoke
//            { unit=testUnit } )
//                //pricing_provider=DexEnum.WINGRIDERS.nativeName} )
//        val testAsset = assetService.getAssetByUnit(testUnit)
//        // Test candle date: 2022-01-01T00:00, 1640966400
//        val testCandleDate = Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.of(2022, 1, 1, 0, 0, 0))
//        println("Test candle date: $testCandleDate, ${testCandleDate.toEpochSecond(Helpers.zoneOffset)}")
//        val testCandleA = CandleDTO(
//            testAsset!!.id,
//            testUnit,
//            testCandleDate.toEpochSecond(Helpers.zoneOffset),
//            9.0,
//            11.0,
//            8.0,
//            10.0,
//            1000.0)
//        val testCandleA2 = testCandleA.copy(unit = "A", time=1707000950)
//        /* This is the min-max we expect to find: 1707000900 */
//        val testCandleB = testCandleA.copy(unit = "B", time=1707000900)
//        val testCandleB2 = testCandleA.copy(unit = "B", time=1707000850)
//        candleService.persistOrUpdate(listOf(testCandleA, testCandleA2, testCandleB, testCandleB2), Duration.ofMinutes(15))
//
//        val testCandleC = testCandleA.copy(unit = "C", time=1707000800)
//        val testCandleC2 = testCandleA.copy(unit = "C", time=1707000750)
//        val testCandleD = testCandleA.copy(unit = "D", time=1707000700)
//        val testCandleD2 = testCandleA.copy(unit = "C", time=1707000650)
//        candleService.persistOrUpdate(listOf(testCandleC, testCandleC2, testCandleD, testCandleD2), Duration.ofHours(1))
//
//        val testCandleE = testCandleA.copy(unit = "E", time=1707000600)
//        val testCandleE2 = testCandleA.copy(unit = "E", time=1707000550)
//        val testCandleF = testCandleA.copy(unit = "F", time=1707000500)
//        val testCandleF2 = testCandleA.copy(unit = "F", time=1707000450)
//        candleService.persistOrUpdate(listOf(testCandleE, testCandleE2, testCandleF, testCandleF2), Duration.ofDays(1))
//
//        val testCandleG = testCandleA.copy(unit = "G", time=1707000400)
//        val testCandleG2 = testCandleA.copy(unit = "G", time=1707000350)
//        val testCandleH = testCandleA.copy(unit = "H", time=1707000300)
//        val testCandleH2 = testCandleA.copy(unit = "H", time=1707000250)
//        candleService.persistOrUpdate(listOf(testCandleG, testCandleG2, testCandleH, testCandleH2), Duration.ofDays(7))
//
//        val lastFifteen = candleService.getLastFifteenCandle(testUnit)
//        println("Latest 15 candle: $lastFifteen")
//        val time = candleService.getSyncPointTime()
//        println("Sync point time: $time")
//        val initState = chainService.determineInitialisationState(null)
//        println("Point auto selected: $initState")
//
//        /* EXPECT
//           test candle time: 1640966400 == slot: 49400109
//           min-max: 1707000900 == Saturday, 3 February 2024 22:55
//           discreteAdjusted: 1707000300 == Saturday, 3 February 2024 22:45:00,
//           discreteAdjustedSlot: 115434009 */
//        assertTrue(initState.chainStartPoint.slot.equals(115434002L))
//        assertTrue(initState.chainStartPoint.hash.equals("9e3478c98f6bf5eacc437fa6f9ab5972d23850450c4d37d801e45e8c72674ff0"))
//    }
//
//    // TODO, mock service
////    @Test
////    fun autoSelectPoint_PriorCandles_2() {
////        baseCandleRepository.truncateAllCandles()
////        val testUnit = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459"
////        // Test candle date: 2022-01-01T00:00, 1640966400
////        val testCandleDate = Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.of(2022, 1, 1, 0, 0, 0))
////        val testCandle = CandleDTO(
////            testUnit,
////            testCandleDate.toEpochSecond(Helpers.zoneOffset),
////            9.0,
////            11.0,
////            8.0,
////            10.0,
////            1000.0)
////        candleService.persistOrUpdate(listOf(testCandle), Duration.ofMinutes(15))
////        val lastFifteen = candleService.getLastFifteenCandle(testUnit)
////        println("Latest 15 candle: $lastFifteen")
////        val syncPointTime = candleService.getSyncPointTime()
////        println("Sync point time: $syncPointTime")
////        val discreteCandleDtgAdjustedTime = Helpers.toNearestDiscreteDate(Duration.ofMinutes(15), LocalDateTime.ofEpochSecond(syncPointTime!!, 0 , Helpers.zoneOffset))
////            .toEpochSecond(Helpers.zoneOffset)
////        /* In this case there is no adjustment since orig candle dtg is aligned to 0 mins */
////        val discreteCandleDtgAdjustedSlot = testCandleDate.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset
////        println("Adjusted time: $discreteCandleDtgAdjustedTime, slot: ${discreteCandleDtgAdjustedTime + Helpers.slotConversionOffset}")
////        /* Mocking getBlockNearestToSlot() */
////        declareMock<ChainDatabaseService> {
////            /* For adjusted slot: "e7e7e46236ef2ac558a9b0a370b1d47c1015ce84c2738282692b0f7729451690"	311	6698517	49400126 */
////            every { getBlockNearestToSlot(discreteCandleDtgAdjustedSlot) } returns BlockView("e7e7e46236ef2ac558a9b0a370b1d47c1015ce84c2738282692b0f7729451690", 311, 6698517L, 49400126L)
////        }
////        val initState = chainService.determineInitialisationState(null)
////        println("Point auto selected: $initState")
////        assertTrue(initState.chainStartPoint.slot.equals(49400126L))
////        assertTrue(initState.chainStartPoint.hash.equals("e7e7e46236ef2ac558a9b0a370b1d47c1015ce84c2738282692b0f7729451690"))
////    }
//}