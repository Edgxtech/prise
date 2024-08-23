package tech.edgx.prise.indexer.service.price

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import tech.edgx.prise.indexer.domain.*
import tech.edgx.prise.indexer.model.PriceHistoryDTO
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.math.BigInteger
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow
import kotlin.math.sqrt

class HistoricalPriceHelpersTest {

    @Test
    fun determineTriggers_None() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Pair(Duration.ofMinutes(15), startStateDtg))
        println("Previous state: $previousCandleDtgState")
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 20, 0))
        println("Next dtg: $nextSlotDtg")
        val triggers = HistoricalPriceHelpers.determineTriggers(true, nextSlotDtg, emptyList(), emptyList(), Helpers.smallestDuration, previousCandleDtgState, false, nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset)
        println("Determined triggers: $triggers")
        assertTrue(triggers.isEmpty())
    }

    /* Happens when in bootstrapping mode and chainsync has passed a candle boundary */
    @Test
    fun determineTriggers_FinaliseOnly() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Pair(Duration.ofMinutes(15), startStateDtg))
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 35, 0))
        val triggers = HistoricalPriceHelpers.determineTriggers(true, nextSlotDtg, emptyList(), emptyList(), Helpers.smallestDuration, previousCandleDtgState, false, nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset)
        println("Determined triggers: $triggers")
        assertTrue(triggers.size==1)
        assertTrue(triggers.first().type== HistoricalPriceHelpers.TriggerType.FINALISE)
        assertTrue(triggers.first().candleDtg==startStateDtg)
    }

    /* Happens when in live sync mode (not bootstrapping) and chainsync has passed a candle boundary */
    @Test
    fun determineTriggers_FinaliseAndInitialiseNext() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Pair(Duration.ofMinutes(15), startStateDtg))
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 35, 0))
        val triggers = HistoricalPriceHelpers.determineTriggers(false, nextSlotDtg, emptyList(), emptyList(), Helpers.smallestDuration, previousCandleDtgState, false, nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset)
        println("Determined triggers: $triggers")
        assertTrue(triggers.size==2)
        val iterator = triggers.iterator()
        val trigger1 = iterator.next()
        assertTrue(trigger1.type==HistoricalPriceHelpers.TriggerType.FINALISE)
        assertTrue(trigger1.candleDtg==startStateDtg)
        val trigger2 = iterator.next()
        assertTrue(trigger2.type==HistoricalPriceHelpers.TriggerType.INITIALISE)
        assertTrue(trigger2.candleDtg==nextSlotDtg)
    }

    /* Happens when in live sync mode (not bootstrapping) and chainsync is still within the existing candle dtg bounds */
    @Test
    fun determineTriggers_Update() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Pair(Duration.ofMinutes(15), startStateDtg))
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 25, 0))
        val triggers = HistoricalPriceHelpers.determineTriggers(false, nextSlotDtg, emptyList(), emptyList(), Helpers.smallestDuration, previousCandleDtgState, false, nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset)
        println("Determined triggers-1: $triggers")
        assertTrue(triggers.isEmpty())

        // Check again with some swaps
        val testSwap = Swap("hash", 0L, 0, "unit1", "unit2", BigInteger.ONE, BigInteger.ONE, 0)
        val triggers2 = HistoricalPriceHelpers.determineTriggers(false, nextSlotDtg, listOf(testSwap), listOf(testSwap), Helpers.smallestDuration, previousCandleDtgState, false, nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset)
        println("Determined triggers-2: $triggers2")
        assertTrue(triggers2.size==1)
        val iterator2 = triggers2.iterator()
        val trigger21 = iterator2.next()
        assertTrue(trigger21.type==HistoricalPriceHelpers.TriggerType.UPDATE)
        assertTrue(trigger21.candleDtg==startStateDtg)
        assertTrue(trigger21.swaps == listOf(testSwap))
    }

    /* Happens when dtg boundary is crossed, it is specifically for the smallest resolution, there are new swaps with the block just received but only need to finalise the previous candle */
    @Test
    fun determineTriggers_FinaliseAndInitialise_NewSwapsAcrossBoundary() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Pair(Duration.ofMinutes(15), startStateDtg))
        val date25m = LocalDateTime.of(2024, 1, 1, 1, 25, 0)
        val date35m = LocalDateTime.of(2024, 1, 1, 1, 35, 0)

        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, date35m)
        val slot25m = date25m.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset
        val slot35m = date35m.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset

        val testSwap25m = Swap("hash", slot25m, 0, "unit1", "unit2", BigInteger.ONE, BigInteger.ONE, 0)
        val testSwap35m = Swap("hash", slot35m, 0, "unit1", "unit2", BigInteger.ONE, BigInteger.ONE, 0)

        val bufferedSwaps = listOf(testSwap25m, testSwap35m)
        val newSwaps = listOf(testSwap35m)

        val triggers = HistoricalPriceHelpers.determineTriggers(false, nextSlotDtg, bufferedSwaps, newSwaps, Helpers.smallestDuration, previousCandleDtgState, false, slot35m)
        println("Determined triggers: $triggers")
        // The first trigger expected to only contain the 25m swap
        // The second trigger expected to only contain the 35m swap
        assertTrue(triggers.size==2)
        val iterator = triggers.iterator()
        val trigger1 = iterator.next()
        assertTrue(trigger1.type==HistoricalPriceHelpers.TriggerType.FINALISE)
        assertTrue(trigger1.candleDtg==startStateDtg)
        assertTrue(trigger1.swaps==listOf(testSwap25m))
        val trigger2 = iterator.next()
        assertTrue(trigger2.type==HistoricalPriceHelpers.TriggerType.INITIALISE)
        assertTrue(trigger2.candleDtg==nextSlotDtg)
        assertTrue(trigger2.swaps== listOf(testSwap35m))
    }

    @Test
    fun makeKnownWeeklyCandle() {
        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingriders
        val fromAsset = Asset.invoke() {
            unit = testUnit
            decimals = 6
        }
        val toAsset = Asset.invoke() {
            unit = "lovelace"
            decimals = 6
        }
        val reader = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv")
            .readText(Charsets.UTF_8).byteInputStream().bufferedReader()
        reader.readLine()
        val swaps: List<Swap> = reader.lineSequence()
            .filter { it.isNotBlank() }
            .map {
                val parts = it.split(",")
                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigInteger(), parts[6].toBigInteger(), parts[7].toInt() )
            }.toList()
        println("Last swap: ${swaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(swaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")
        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(swaps, fromAsset, toAsset)
        println("Last transformed price: ${rawPrices?.last()}, last raw price date: ${rawPrices?.last()?.ldt?.toEpochSecond(Helpers.zoneOffset)}")
        println("Raw converted prices, #: ${rawPrices?.size}")
        val candle = rawPrices?.let {
            HistoricalPriceHelpers.calculateCandleFromSwaps(
                it,
                fromAsset,
                null,
                LocalDateTime.ofEpochSecond(115179309,0,Helpers.zoneOffset),
                //null
            ) }
        candle?.time?.equals(115179309L)?.let { assertTrue(it) }
        candle?.open?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.high?.equals(0.14993549140025805)?.let { assertTrue(it) }
        candle?.low?.equals(0.12252836930272085)?.let { assertTrue(it) }
        candle?.close?.equals(0.12361034418332056)?.let { assertTrue(it) }
        candle?.volume?.equals(527051.0194070002)?.let { assertTrue(it) }
    }

    @Test
    fun makeKnownDailyCandle() {
        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingriders
        val fromAsset = Asset.invoke() {
            unit = testUnit
            decimals = 6
        }
        val toAsset = Asset.invoke() {
            unit = "lovelace"
            decimals = 6
        }
        val csvString: String = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv").readText(Charsets.UTF_8)
        val reader = csvString.byteInputStream().bufferedReader()
        reader.readLine()
        val swaps: List<Swap> = reader.lineSequence()
            .filter { it.isNotBlank() }
            .map {
                val parts = it.split(",")
                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigInteger(), parts[6].toBigInteger(), parts[7].toInt() )
            }
            .filter { it.slot in 112500909..112587308 } // Filter to 01 Jan 24 DAY only
            .toList()
        println("Last swap: ${swaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(swaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")
        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(swaps, fromAsset, toAsset)
        println("Last transformed price: ${rawPrices?.last()}, last raw price date: ${rawPrices?.last()?.ldt?.toEpochSecond(Helpers.zoneOffset)}")
        println("Raw converted prices, #: ${rawPrices?.size}")
        val candle = rawPrices?.let {
            HistoricalPriceHelpers.calculateCandleFromSwaps(
                it,
                fromAsset,
                null,
                LocalDateTime.ofEpochSecond(1704153600,0,Helpers.zoneOffset),
                //null
            ) }
        candle?.time?.equals(1704153600L)?.let { assertTrue(it) }
        candle?.open?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.high?.equals(0.13527505585660082)?.let { assertTrue(it) }
        candle?.low?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.close?.equals(0.13347049308608286)?.let { assertTrue(it) }
        candle?.volume?.equals(19353.290609)?.let { assertTrue(it) }
    }

    @Test
    fun makeKnownHourlyCandle() {
        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingrdiers
        val fromAsset = Asset.invoke() {
            unit = testUnit
            decimals = 6
        }
        val toAsset = Asset.invoke() {
            unit = "lovelace"
            decimals = 6
        }
        val csvString: String = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv").readText(Charsets.UTF_8)
        val reader = csvString.byteInputStream().bufferedReader()
        reader.readLine()
        val swaps: List<Swap> = reader.lineSequence()
            .filter { it.isNotBlank() }
            .map {
                val parts = it.split(",")
                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigInteger(), parts[6].toBigInteger(), parts[7].toInt() )
            }
            .filter { it.slot in 112500909..112504509 } // Filter to 01 Jan 00:00 to 01:00 ONLY
            .toList()
        println("Last swap: ${swaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(swaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")
        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(swaps, fromAsset, toAsset)
        println("Last transformed price: ${rawPrices?.last()}, last raw price date: ${rawPrices?.last()?.ldt?.toEpochSecond(Helpers.zoneOffset)}")
        println("Raw converted prices, #: ${rawPrices?.size}")
        val candle = rawPrices?.let {
            HistoricalPriceHelpers.calculateCandleFromSwaps(
                it,
                fromAsset,
                null,
                LocalDateTime.ofEpochSecond(1704070800,0,Helpers.zoneOffset),
                //null
            ) }
        candle?.time?.equals(1704070800L)?.let { assertTrue(it) }
        candle?.open?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.high?.equals(0.13527505585660082)?.let { assertTrue(it) }
        candle?.low?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.close?.equals(0.13486224761904764)?.let { assertTrue(it) }
        candle?.volume?.equals(8307.623372)?.let { assertTrue(it) }
    }

    @Test
    fun makeKnownFifteenCandle() {
        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingrdiers
        val fromAsset = Asset.invoke() {
            unit = testUnit
            decimals = 6
        }
        val toAsset = Asset.invoke() {
            unit = "lovelace"
            decimals = 6
        }
        val csvString: String = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv").readText(Charsets.UTF_8)
        val reader = csvString.byteInputStream().bufferedReader()
        reader.readLine()
        val swaps: List<Swap> = reader.lineSequence()
            .filter { it.isNotBlank() }
            .map {
                val parts = it.split(",")
                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigInteger(), parts[6].toBigInteger(), parts[7].toInt() )
            }
            .filter { it.slot in 112500909..112501809 } // Filter to 01 Jan 00:00 to 00:15 ONLY
            .toList()
        println("Last swap: ${swaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(swaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")
        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(swaps, fromAsset, toAsset)
        println("Last transformed price: ${rawPrices?.last()}, last raw price date: ${rawPrices?.last()?.ldt?.toEpochSecond(Helpers.zoneOffset)}")
        println("Raw converted prices, #: ${rawPrices?.size}")
        val candle = rawPrices?.let {
            HistoricalPriceHelpers.calculateCandleFromSwaps(
                it,
                fromAsset,
                null,
                LocalDateTime.ofEpochSecond(1704068100,0,Helpers.zoneOffset),
                //null
            ) }
        candle?.time?.equals(1704068100L)?.let { assertTrue(it) }
        candle?.open?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.high?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.low?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.close?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.volume?.equals(11.396201)?.let { assertTrue(it) }
    }

    @Test
    fun makeKnownHourlyCandleFromSubCandles() {
        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingrdiers
        val fromAsset = Asset.invoke() {
            unit = testUnit
            decimals = 6
        }
        val toAsset = Asset.invoke() {
            unit = "lovelace"
            decimals = 6
        }
        val csvString: String = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv").readText(Charsets.UTF_8)
        val reader = csvString.byteInputStream().bufferedReader()
        reader.readLine()
        val swaps: List<Swap> = reader.lineSequence()
            .filter { it.isNotBlank() }
            .map {
                val parts = it.split(",")
                Swap(txHash = parts[0], parts[1].toLong(), parts[2].toInt(), parts[3], parts[4], parts[5].toBigInteger(), parts[6].toBigInteger(), parts[7].toInt() )
            }
            .filter { it.slot in 112500909..112504509 } // Filter to 01 Jan 00:00 to 01:00 ONLY
            .toList()
        println("Last swap: ${swaps.last()}, timestamp: ${LocalDateTime.ofEpochSecond(swaps.last().slot - Helpers.slotConversionOffset, 0, Helpers.zoneOffset)}")
        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(swaps, fromAsset, toAsset)
        println("Last transformed price: ${rawPrices?.last()}, last raw price date: ${rawPrices?.last()?.ldt?.toEpochSecond(Helpers.zoneOffset)}, Raw converted prices, #: ${rawPrices?.size}")
        val subCandles = mutableListOf<CandleDTO>()
        for (time in  1704067200L..1704070800L step 900) { // 15 minute steps
            println("Time: $time")
            val candle = rawPrices
                ?.filter {
                    println("Filtering; ${it.ldt.toEpochSecond(Helpers.zoneOffset)}, time: $time to ${time.plus(900)}")
                    it.ldt.toEpochSecond(Helpers.zoneOffset) in time .. time.plus(900) }
                ?.let {
                    println("# filtered prices: ${it.size}")
                    if (!it.isNullOrEmpty()) {
                        HistoricalPriceHelpers.calculateCandleFromSwaps(
                            it,
                            fromAsset,
                            null,
                            LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                            //null
                        )
                    } else null }
            if (candle != null) {
                subCandles.add(candle)
            }
        }
        val candle = HistoricalPriceHelpers.calculateCandleFromSubCandles(
            subCandles.map { LatestCandlesView(it.symbol, it.time, it.open, it.high, it.low, it.close, it.volume, "15m") },
            testUnit,
            null,
            LocalDateTime.ofEpochSecond(1704070800L, 0, Helpers.zoneOffset))
        candle?.time?.equals(1704070800L)?.let { assertTrue(it) }
        candle?.open?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.high?.equals(0.13527505585660082)?.let { assertTrue(it) }
        candle?.low?.equals(0.13328890993840303)?.let { assertTrue(it) }
        candle?.close?.equals(0.13486224761904764)?.let { assertTrue(it) }
        candle?.volume?.equals(8307.623372)?.let { assertTrue(it) }
    }

    @Test
    fun filterOutliers_GrubsTest() {
        val duration = Duration.ofMinutes(15)
        val rawPrices = mutableListOf<PriceHistoryDTO>()
        val outlierFrequency = 25
        for ((count, time) in (1661699696L..1701699696L step 20).withIndex()) {
            if (count % outlierFrequency == 0) {
                // Add an outlier
                rawPrices.add(
                    PriceHistoryDTO(
                        LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                        (Math.random() + 30.0),
                        Math.random() * 10000
                    )
                )
            } else { // Add a random val in the range: 1 -> 2
                rawPrices.add(
                    PriceHistoryDTO(
                        LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                        (Math.random() + 1.0),
                        Math.random() * 10000
                    )
                )
            }
        }
        println("# raw prices: ${rawPrices.size}")
        val groupedRawPrices: Map<LocalDateTime, List<PriceHistoryDTO>> = rawPrices.groupBy { p -> Helpers.toNearestDiscreteDate(duration, p.ldt) }
        println("grouped raw prices : ${groupedRawPrices.keys.size}")
        val groupedSortedPrices: TreeMap<LocalDateTime, List<PriceHistoryDTO>> = TreeMap<LocalDateTime, List<PriceHistoryDTO>>(groupedRawPrices)
        val expectedRatio = 1 - 1.div(outlierFrequency.toDouble())
        groupedSortedPrices.forEach candlegrouploop@{ _, groupPrices ->
            println("# prices before filtering, #: ${groupPrices.size}")
            val filteredPrices = HistoricalPriceHelpers.filterOutliersByGrubbsTest(groupPrices, null)
            println("# prices after filtering, #: ${filteredPrices.size}")
            val convertedRatio = filteredPrices.size.div(groupPrices.size.toDouble())
            println("expected ratio: $expectedRatio vs converted ratio: $convertedRatio")
            val comparison = convertedRatio.div(expectedRatio)
            // should remove ~ 1/outlierFrequency
            println("Comparison: $comparison")
            //assertTrue(comparison < 1.05 && comparison > 0.95) // Can tighten thresholds with higher qty data points / adjusting the step
            assertTrue(comparison < 1.15 && comparison > 0.85)
        }
    }

    @Test
    fun filterOutliersGrubbsTest() {
        val lastCandle = CandleDTO(symbol="1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", time=1679096700, open=0.5211294875189929, high=0.5211294875189929, low=0.5211294875189929, close=0.5211294875189929, volume=4801.687098) //, resolution="15m")
        val prices = listOf(PriceHistoryDTO(price=1.045992262857252, volume=978.748416),
            PriceHistoryDTO(price=1.045992262857252, volume=978.748416),
            PriceHistoryDTO(price=0.5208855320909522, volume=2025.87217),
            PriceHistoryDTO(price=0.5236808222822782, volume=490.009618),
            PriceHistoryDTO(price=0.5237673315190541, volume=99.988559))
//        PriceHistoryDTO(price=0.5211294875189929, volume=-1.0),
//        PriceHistoryDTO(price=0.5211294875189929, volume=-1.0),
//        PriceHistoryDTO(price=0.5211294875189929, volume=-1.0),
//        PriceHistoryDTO(price=0.5211294875189929, volume=-1.0),
//        PriceHistoryDTO(price=0.5211294875189929, volume=-1.0))
        val filteredPrices = HistoricalPriceHelpers.filterOutliersByGrubbsTest(prices, lastCandle)
        println("RawPrices: $prices")
        println("Filtered Prices: $filteredPrices")
    }

    @Test
    fun filterOutliers_ExponentialMovingAverageTest() {
        val now = LocalDateTime.now()
        val data = listOf(
            PriceHistoryDTO(now,1.0,0.0),
            PriceHistoryDTO(now,2.0,0.0),
            PriceHistoryDTO(now,3.0,0.0),
            PriceHistoryDTO(now,4.0,0.0),
            PriceHistoryDTO(now,5.0,0.0),
            PriceHistoryDTO(now,6.0,0.0),
            PriceHistoryDTO(now,7.0,0.0),
            PriceHistoryDTO(now,8.0,0.0),
            PriceHistoryDTO(now,9.0,0.0),
            PriceHistoryDTO(now,10.0,0.0))
        // Initialise mutable ema, variance
        var ema = data.map { it.price!! }.average()
        var variance = data.map { (it.price!! - ema).pow(2) }.average()
        println("Initial EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        val badValues = mutableListOf<Double>()
        val retainedValues = mutableListOf<Double>()
        for (i in 1..100) {
            val randomNewVal = if (i%20==0) {
                val badValue = (50..100).random().toDouble()
                badValues.add(badValue)
                badValue
            } else (1..10).random().toDouble()
            val filtered = HistoricalPriceHelpers.filterOutliersByEMATest(listOf(PriceHistoryDTO(now, randomNewVal, 0.0)), ema, variance)
            retainedValues.addAll(filtered.map { it.price!! })
            /* update the EMA state */
            HistoricalPriceHelpers.getNextEma(ema, variance, randomNewVal).let { (first, second) -> ema = first; variance = second }
            println("After value ($randomNewVal): next EMA: ${ema}, variance: $variance, stddev: ${sqrt(variance)}")
        }
        println("Retained values: $retainedValues")
        badValues.forEach {
            println("Checking no bad value was retained: $it")
            assertTrue(!retainedValues.contains(it))
        }
    }

    @Test
    fun filterOutliers_ExponentialMovingAverageTest_PrimitiveDT() {
        val data = listOf(1,2,3,4,5,6,7,8,9,10)
        val movingAverage = data.windowed(3,1) { it.average() }
        // Initialise mutable ema, variance
        var ema = movingAverage.last()
        var variance = data.map { (it - ema).pow(2) }.average()
        println("Initial EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        val badValues = mutableListOf<Double>()
        val filteredValues = mutableListOf<Double>()
        for (i in 1..100) {
            val randomNewVal = if (i%20==0) {
                val badValue = (50..100).random().toDouble()
                badValues.add(badValue)
                badValue
            } else (1..10).random().toDouble()
            val shouldFilterValue = (ema - randomNewVal).absoluteValue > 2*sqrt(variance)
            if (shouldFilterValue) filteredValues.add(randomNewVal)
            println("Should filter (val: $randomNewVal, ema: $ema, stddev:${sqrt(variance)})?: ${shouldFilterValue}")
            /* update the EMA state */
            HistoricalPriceHelpers.getNextEma(ema, variance, randomNewVal).let { (first, second) -> ema = first; variance= second }
            println("After value ($randomNewVal): next EMA: ${ema}, variance: $variance, stddev: ${sqrt(variance)}")
        }
        println("Filtered values: $filteredValues")
        badValues.forEach {
            println("Checking bad value was identified: $it")
            assertTrue(filteredValues.contains(it))
        }
    }

    @Test
    fun determineClosePrice_NoSlotDuplicates() {
        val filteredGroupPrices = mutableListOf<PriceHistoryDTO>()
        for (time in 1661699696L..1701699696L step 100) {
            filteredGroupPrices.add(
                PriceHistoryDTO(
                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                    (Math.random() + 20.0),
                    Math.random() * 10000
                )
            )
        }
        val close = HistoricalPriceHelpers.determineClosePrice(filteredGroupPrices)
        assertTrue(close?.equals(filteredGroupPrices.last().price)?: false)
    }

    @Test
    fun determineClosePrice_WithSlotDuplicates() {
        val filteredGroupPrices = mutableListOf<PriceHistoryDTO>()
        for (time in 1661699696L..1701699696L step 100) {
            filteredGroupPrices.add(
                PriceHistoryDTO(
                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                    (Math.random() + 20.0),
                    Math.random() * 10000
                )
            )
        }
        val lastPH = filteredGroupPrices.last()
        filteredGroupPrices.add(PriceHistoryDTO(
            lastPH.ldt,
            lastPH.price?.minus(0.1),
            lastPH.volume
        ))
        filteredGroupPrices.add(PriceHistoryDTO(
            lastPH.ldt,
            lastPH.price?.minus(0.5), // expect this is selected since is lowest val of the last slot group
            lastPH.volume
        ))
        filteredGroupPrices.add(PriceHistoryDTO(
            lastPH.ldt,
            lastPH.price?.plus(0.5),
            lastPH.volume
        ))
        val close = HistoricalPriceHelpers.determineClosePrice(filteredGroupPrices)
        println("determined close: $close, vs expected: ${lastPH.price?.minus(0.5)}")
        assertTrue(close?.equals(lastPH.price?.minus(0.5))?: false)
        assertFalse(close?.equals(lastPH.price?.minus(0.1))?: false)
        assertFalse(close?.equals(lastPH.price?.plus(0.5))?: false)
    }

    @Test
    fun determineOpenPrice() {
        val duration = Duration.ofDays(7)
        val rawPrices = mutableListOf<PriceHistoryDTO>()
        for (time in 1671699696L..1701699696L step 10) {
            rawPrices.add(
                PriceHistoryDTO(
                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                    (Math.random() + 20.0),
                    Math.random() * 10000
                )
            )
        }
        val groupedRawPrices: Map<LocalDateTime, List<PriceHistoryDTO>> = rawPrices.groupBy { p -> Helpers.toNearestDiscreteDate(duration, p.ldt) }
        val groupedSortedPrices: TreeMap<LocalDateTime, List<PriceHistoryDTO>> = TreeMap<LocalDateTime, List<PriceHistoryDTO>>(groupedRawPrices)
        groupedSortedPrices.forEach candlegrouploop@{ (dtgKey, groupPrices) ->
            /* Debugging, make sure is sorted */
            //val sortedFirst = groupPrices.sortedBy { gp -> gp.ldt }[0].price
            //println("check group prices[0]: ${groupPrices[0].price} is first?: ${sortedFirst}: ${sortedFirst.equals(groupPrices[0].price)}")
            var lastCandle: CandleDTO? = null
            var open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle?.close, dtgKey, null)
            assertNull(open)

            // Case: Null lastCandle, non-null first group price, expect first group price
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle?.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(groupPrices[0].price))

            /* make a random prior candle, expect open to be lastClose (if lastClose not null), otherwise be first val in group
             * UNLESS, the dtgKey is the for the exact time as the lastCandle itself, in which case retain its existing open price */

            // Case: Not same dtgKey as lastCandle, non-null lastClose, expect last close
            lastCandle = CandleDTO("", 1234567, 11.0, 15.0, 8.0, 10.0, 0.0) //, "15m")
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(lastCandle.close))

            // Case: Not same dtgKey as lastCandle, null lastClose, expect first group price
            lastCandle = CandleDTO("", 1234567, 11.0, 15.0, 8.0, null, 0.0) //, "15m")
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(groupPrices[0].price))

            // Case: Same DTG key, expect retain its open
            lastCandle = CandleDTO("", dtgKey.toEpochSecond(Helpers.zoneOffset), 11.0, 15.0, 8.0, 11.0, 0.0) //, "15m")
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(lastCandle.open))
        }
        println("checked open price finder for # candle groups: ${groupedSortedPrices.size}")
    }

    @Test
    fun transformTradesToPrices() {
        val swaps = listOf(
            Swap(
                "",
                49400109,
                2,
                asset1Unit = "null",
                asset2Unit = "null",
                BigInteger.valueOf(100),
                BigInteger.valueOf(200),
                0
            ),
            Swap(
                "",
                49400209,
                2,
                asset1Unit = "null",
                asset2Unit = "null",
                BigInteger.valueOf(100),
                BigInteger.valueOf(210),
                0
            ),
            Swap(
                "",
                49400309,
                2,
                asset1Unit = "null",
                asset2Unit = "null",
                BigInteger.valueOf(100),
                BigInteger.valueOf(220),
                0
            )
        )
        val fromAsset = Asset.invoke()
        fromAsset.decimals = 6
        val toAsset = Asset.invoke()
        toAsset.decimals = 6
        val convertedPriceHistory = HistoricalPriceHelpers.transformTradesToPrices(
            swaps,
            fromAsset,
            toAsset
        )
        println("converted to $convertedPriceHistory")
        assertTrue(convertedPriceHistory?.size==3)
        assertTrue(if (convertedPriceHistory != null) convertedPriceHistory[0].price==0.5 else false)
        convertedPriceHistory?.forEachIndexed { idx, cph ->
            assertTrue(cph.price == swaps[idx].amount1.toDouble().div(10.0.pow(toAsset.decimals!!.toDouble())).div(swaps[idx].amount2.toDouble().div(Math.pow(10.0, fromAsset.decimals!!.toDouble()))))
        }
        assertTrue(
            swaps.map { swap -> swap.amount1.toDouble().div(10.0.pow(toAsset.decimals!!)) }.reduce{ a, b -> java.lang.Double.sum(a, b)} ==
                convertedPriceHistory?.map { p -> p.volume }?.reduce{ a: Double, b: Double -> java.lang.Double.sum(a, b) })
    }

    @Test
    fun makeRandomCandles() {
        val testUnit = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459" // INDY
        val fromAsset = Asset.invoke() { unit = testUnit }
        val rawPrices = mutableListOf<PriceHistoryDTO>()
        for (time in 1661699696L..1701699696L step 100) {
            rawPrices.add(
                PriceHistoryDTO(
                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                    (Math.random() + 1.0),
                    Math.random()*10000
                )
            )
        }
        val candles = HistoricalPriceHelpers.calculateCandleFromSwaps(
            rawPrices,
            fromAsset,
            null,
            Helpers.toNearestDiscreteDate(Duration.ofDays(7), LocalDateTime.ofEpochSecond(1701699696L, 0, Helpers.zoneOffset)),
            //null
        )
        println("Calculated candles: $candles")
    }

    @Test
    fun makeSuperCandles_HourlyFromFifteen() {
        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d507357696e67526964657273" // WINGRIDERS
        /* since gap is 900s, these are 15m candles */
        val subCandles = listOf<LatestCandlesView>(
            LatestCandlesView(testUnit, 1707200100, 0.11880426606211597, 0.11880426606211597, 0.11880426606211597, 0.11880426606211597, 0.0, "15m"),
            LatestCandlesView(testUnit, 1707201000, 0.11989126875599858, 0.11989126875599858, 0.11989126875599858, 0.11879393890857086, 20.116965, "15m"),
            LatestCandlesView(testUnit, 1707201900, 0.11879393890857086, 0.11879393890857086, 0.11879393890857086, 0.11879393890857086, 0.0, "15m"),
            LatestCandlesView(testUnit, 1707202800, 0.11989126875599858, 0.11989126875599858, 0.1197302362074337, 0.1197302362074337, 1017.0, "15m")
        )
        val candle = HistoricalPriceHelpers.calculateCandleFromSubCandles(
            subCandles,
            testUnit,
            null,
            LocalDateTime.ofEpochSecond(1707202800L, 0, Helpers.zoneOffset)
        )
        println("Calculated candle: $candle")
        assertTrue(candle?.volume==1037.116965)
        assertTrue(candle?.open==0.11880426606211597)
        assertTrue(candle?.high==0.11989126875599858)
        assertTrue(candle?.low==0.11879393890857086)
        assertTrue(candle?.close==0.1197302362074337)
    }
}