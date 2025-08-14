package tech.edgx.prise.indexer.service.price

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import tech.edgx.prise.indexer.domain.*
import tech.edgx.prise.indexer.model.PriceDTO
import tech.edgx.prise.indexer.model.prices.CandleDTO
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.math.BigDecimal
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
        val previousCandleDtgState = mapOf(Duration.ofMinutes(15) to startStateDtg)
        println("Previous state: $previousCandleDtgState")
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 20, 0))
        println("Next dtg: $nextSlotDtg")
        val triggers = HistoricalPriceHelpers.determineTriggers(
            isBootstrapping = true,
            currentDtg = nextSlotDtg,
            bufferedPrices = emptyList(),
            blockPrices = emptyList(),
            duration = Helpers.smallestDuration,
            previousCandleDtgState = previousCandleDtgState,
            isSubsequentTrigger = false,
            slot = nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset
        )
        println("Determined triggers: $triggers")
        assertTrue(triggers.isEmpty())
    }

    @Test
    fun determineTriggers_FinaliseOnly() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Duration.ofMinutes(15) to startStateDtg)
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 35, 0))
        val triggers = HistoricalPriceHelpers.determineTriggers(
            isBootstrapping = true,
            currentDtg = nextSlotDtg,
            bufferedPrices = emptyList(),
            blockPrices = emptyList(),
            duration = Helpers.smallestDuration,
            previousCandleDtgState = previousCandleDtgState,
            isSubsequentTrigger = false,
            slot = nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset
        )
        println("Determined triggers: $triggers")
        assertEquals(1, triggers.size)
        assertEquals(HistoricalPriceHelpers.TriggerType.FINALISE, triggers.first().type)
        assertEquals(startStateDtg, triggers.first().candleDtg)
        assertTrue(triggers.first().prices.isEmpty())
    }

    @Test
    fun determineTriggers_FinaliseAndInitialiseNext() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Duration.ofMinutes(15) to startStateDtg)
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 35, 0))
        val triggers = HistoricalPriceHelpers.determineTriggers(
            isBootstrapping = false,
            currentDtg = nextSlotDtg,
            bufferedPrices = emptyList(),
            blockPrices = emptyList(),
            duration = Helpers.smallestDuration,
            previousCandleDtgState = previousCandleDtgState,
            isSubsequentTrigger = false,
            slot = nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset
        )
        println("Determined triggers: $triggers")
        assertEquals(2, triggers.size)
        val iterator = triggers.iterator()
        val trigger1 = iterator.next()
        assertEquals(HistoricalPriceHelpers.TriggerType.FINALISE, trigger1.type)
        assertEquals(startStateDtg, trigger1.candleDtg)
        assertTrue(trigger1.prices.isEmpty())
        val trigger2 = iterator.next()
        assertEquals(HistoricalPriceHelpers.TriggerType.INITIALISE, trigger2.type)
        assertEquals(nextSlotDtg, trigger2.candleDtg)
        assertTrue(trigger2.prices.isEmpty())
    }

    @Test
    fun determineTriggers_Update() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Duration.ofMinutes(15) to startStateDtg)
        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, LocalDateTime.of(2024, 1, 1, 1, 25, 0))
        val triggers = HistoricalPriceHelpers.determineTriggers(
            isBootstrapping = false,
            currentDtg = nextSlotDtg,
            bufferedPrices = emptyList(),
            blockPrices = emptyList(),
            duration = Helpers.smallestDuration,
            previousCandleDtgState = previousCandleDtgState,
            isSubsequentTrigger = false,
            slot = nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset
        )
        println("Determined triggers-1: $triggers")
        assertTrue(triggers.isEmpty())

        // Check with some prices
        val testPrice = Price {
            provider = 0
            asset_id = 1L
            quote_asset_id = 2L
            price = 0.5
            time = nextSlotDtg.toEpochSecond(Helpers.zoneOffset)
            //tx_hash = "hash"
            tx_id = 1L
            amount1 = BigDecimal("100")
            amount2 = BigDecimal("200")
            operation = 0
        }
        val triggers2 = HistoricalPriceHelpers.determineTriggers(
            isBootstrapping = false,
            currentDtg = nextSlotDtg,
            bufferedPrices = listOf(testPrice),
            blockPrices = listOf(testPrice),
            duration = Helpers.smallestDuration,
            previousCandleDtgState = previousCandleDtgState,
            isSubsequentTrigger = false,
            slot = nextSlotDtg.toEpochSecond(Helpers.zoneOffset) + Helpers.slotConversionOffset
        )
        println("Determined triggers-2: $triggers2")
        assertEquals(1, triggers2.size)
        val iterator2 = triggers2.iterator()
        val trigger21 = iterator2.next()
        assertEquals(HistoricalPriceHelpers.TriggerType.UPDATE, trigger21.type)
        assertEquals(startStateDtg, trigger21.candleDtg)
        assertEquals(listOf(testPrice), trigger21.prices)
    }

    @Test
    fun determineTriggers_FinaliseAndInitialise_NewPricesAcrossBoundary() {
        val startStateDtg = LocalDateTime.of(2024, 1, 1, 1, 15, 0)
        val previousCandleDtgState = mapOf(Duration.ofMinutes(15) to startStateDtg)
        val date25m = LocalDateTime.of(2024, 1, 1, 1, 25, 0)
        val date35m = LocalDateTime.of(2024, 1, 1, 1, 35, 0)

        val nextSlotDtg = Helpers.toNearestDiscreteDate(Helpers.smallestDuration, date35m)
        val slot25m = date25m.toEpochSecond(Helpers.zoneOffset)
        val slot35m = date35m.toEpochSecond(Helpers.zoneOffset)

        val testPrice25M = Price {
            provider = 0
            asset_id = 1L
            quote_asset_id = 2L
            price = 0.5
            time = slot25m
            //tx_hash = "hash"
            amount1 = BigDecimal("100")
            amount2 = BigDecimal("200")
            operation = 0
        }
        val testPrice35M = Price {
            provider = 0
            asset_id = 1L
            quote_asset_id = 2L
            price = 0.5
            time = slot35m
            //tx_hash = "hash"
            amount1 = BigDecimal("100")
            amount2 = BigDecimal("200")
            operation = 0
        }

        val bufferedPrices = listOf(testPrice25M, testPrice35M)
        val newPrices = listOf(testPrice35M)

        val triggers = HistoricalPriceHelpers.determineTriggers(
            isBootstrapping = false,
            currentDtg = nextSlotDtg,
            bufferedPrices = bufferedPrices,
            blockPrices = newPrices,
            duration = Helpers.smallestDuration,
            previousCandleDtgState = previousCandleDtgState,
            isSubsequentTrigger = false,
            slot = slot35m
        )
        println("Determined triggers: $triggers")
        assertEquals(2, triggers.size)
        val iterator = triggers.iterator()
        val trigger1 = iterator.next()
        assertEquals(HistoricalPriceHelpers.TriggerType.FINALISE, trigger1.type)
        assertEquals(startStateDtg, trigger1.candleDtg)
        assertEquals(listOf(testPrice25M), trigger1.prices)
        val trigger2 = iterator.next()
        assertEquals(HistoricalPriceHelpers.TriggerType.INITIALISE, trigger2.type)
        assertEquals(nextSlotDtg, trigger2.candleDtg)
        assertEquals(listOf(testPrice35M), trigger2.prices)
    }

//    @Test
//    fun makeKnownWeeklyCandle() {
//        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingriders
//        val fromAsset = Asset.invoke {
//            unit = testUnit
//            decimals = 6
//        }
//        val toAsset = Asset.invoke {
//            unit = "lovelace"
//            decimals = 6
//        }
//        val reader = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv")
//            .readText(Charsets.UTF_8).byteInputStream().bufferedReader()
//        reader.readLine()
//        val prices: List<Price> = reader.lineSequence()
//            .filter { it.isNotBlank() }
//            .map {
//                val parts = it.split(",")
//                Price {
//                    provider = parts[2].toInt()
//                    asset_id = 1L
//                    quote_asset_id = 2L
//                    price = Helpers.calculatePriceInAsset1(
//                        parts[5].toBigDecimal(),
//                        toAsset.decimals!!,
//                        parts[6].toBigDecimal(),
//                        fromAsset.decimals!!
//                    )
//                    time = parts[1].toLong() - Helpers.slotConversionOffset
//                    //tx_hash = parts[0]
//                    amount1 = parts[5].toBigDecimal()
//                    amount2 = parts[6].toBigDecimal()
//                    operation = parts[7].toInt()
//                }
//            }.toList()
//        println("Last price: ${prices.last()}, timestamp: ${LocalDateTime.ofEpochSecond(prices.last().time, 0, Helpers.zoneOffset)}")
//        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(prices, toAsset)
//        println("Last transformed price: ${rawPrices.last()}, last raw price date: ${rawPrices.last().ldt.toEpochSecond(Helpers.zoneOffset)}")
//        println("Raw converted prices, #: ${rawPrices.size}")
//        val candle = HistoricalPriceHelpers.calculateCandleFromSwaps(
//            rawPrices,
//            fromAsset,
//            toAsset,
//            lastCandle =
//            LocalDateTime.ofEpochSecond(115179309, 0, Helpers.zoneOffset)
//        )
//        assertNotNull(candle)
//        assertEquals(115179309L, candle?.time)
//        assertEquals(0.13328890993840303, candle!!.open, 1e-10)
//        assertEquals(0.14993549140025805, candle.high, 1e-10)
//        assertEquals(0.12252836930272085, candle.low, 1e-10)
//        candle.close?.let { assertEquals(0.12361034418332056, it, 1e-10) }
//        assertEquals(527051.0194070002, candle.volume, 1e-10)
//    }

    // TODO, adapt to pull data from saved prices
//    @Test
//    fun makeKnownDailyCandle() {
//        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingriders
//        val fromAsset = Asset.invoke {
//            unit = testUnit
//            decimals = 6
//        }
//        val toAsset = Asset.invoke {
//            unit = "lovelace"
//            decimals = 6
//        }
//        val csvString: String = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv").readText(Charsets.UTF_8)
//        val reader = csvString.byteInputStream().bufferedReader()
//        reader.readLine()
//        val prices: List<Price> = reader.lineSequence()
//            .filter { it.isNotBlank() }
//            .map {
//                val parts = it.split(",")
//                Price {
//                    provider = parts[2].toInt()
//                    asset_id = 1L
//                    quote_asset_id = 2L
//                    price = Helpers.calculatePriceInAsset1(
//                        parts[5].toBigDecimal(),
//                        toAsset.decimals!!,
//                        parts[6].toBigDecimal(),
//                        fromAsset.decimals!!
//                    )
//                    dtg = parts[1].toLong() - Helpers.slotConversionOffset
//                    tx_hash = parts[0]
//                    amount1 = parts[5].toBigDecimal()
//                    amount2 = parts[6].toBigDecimal()
//                    operation = parts[7].toInt()
//                }
//            }
//            .filter { it.dtg in 112500909..112587308 } // Filter to 01 Jan 24 DAY only
//            .toList()
//        println("Last price: ${prices.last()}, timestamp: ${LocalDateTime.ofEpochSecond(prices.last().dtg, 0, Helpers.zoneOffset)}")
//        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(prices, fromAsset, toAsset)
//        println("Last transformed price: ${rawPrices.last()}, last raw price date: ${rawPrices.last().ldt.toEpochSecond(Helpers.zoneOffset)}")
//        println("Raw converted prices, #: ${rawPrices.size}")
//        val candle = HistoricalPriceHelpers.calculateCandleFromSwaps(
//            rawPrices,
//            fromAsset,
//            null,
//            LocalDateTime.ofEpochSecond(1704153600, 0, Helpers.zoneOffset)
//        )
//        assertNotNull(candle)
//        assertEquals(1704153600L, candle?.time)
//        assertEquals(0.13328890993840303, candle!!.open, 1e-10)
//        assertEquals(0.13527505585660082, candle.high, 1e-10)
//        assertEquals(0.13328890993840303, candle.low, 1e-10)
//        candle.close?.let { assertEquals(0.13347049308608286, it, 1e-10) }
//        assertEquals(19353.290609, candle.volume, 1e-10)
//    }

//    @Test
//    fun makeKnownHourlyCandle() {
//        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingriders
//        val fromAsset = Asset.invoke {
//            unit = testUnit
//            decimals = 6
//        }
//        val toAsset = Asset.invoke {
//            unit = "lovelace"
//            decimals = 6
//        }
//        val csvString: String = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv").readText(Charsets.UTF_8)
//        val reader = csvString.byteInputStream().bufferedReader()
//        reader.readLine()
//        val prices: List<Price> = reader.lineSequence()
//            .filter { it.isNotBlank() }
//            .map {
//                val parts = it.split(",")
//                Price {
//                    provider = parts[2].toInt()
//                    asset_id = 1L
//                    quote_asset_id = 2L
//                    price = Helpers.calculatePriceInAsset1(
//                        parts[5].toBigDecimal(),
//                        toAsset.decimals!!,
//                        parts[6].toBigDecimal(),
//                        fromAsset.decimals!!
//                    )
//                    dtg = parts[1].toLong() - Helpers.slotConversionOffset
//                    tx_hash = parts[0]
//                    amount1 = parts[5].toBigDecimal()
//                    amount2 = parts[6].toBigDecimal()
//                    operation = parts[7].toInt()
//                }
//            }
//            .filter { it.dtg in 112500909..112504509 } // Filter to 01 Jan 00:00 to 01:00 ONLY
//            .toList()
//        println("Last price: ${prices.last()}, timestamp: ${LocalDateTime.ofEpochSecond(prices.last().dtg, 0, Helpers.zoneOffset)}")
//        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(prices, fromAsset, toAsset)
//        println("Last transformed price: ${rawPrices.last()}, last raw price date: ${rawPrices.last().ldt.toEpochSecond(Helpers.zoneOffset)}")
//        println("Raw converted prices, #: ${rawPrices.size}")
//        val candle = HistoricalPriceHelpers.calculateCandleFromSwaps(
//            rawPrices,
//            fromAsset,
//            null,
//            LocalDateTime.ofEpochSecond(1704070800, 0, Helpers.zoneOffset)
//        )
//        assertNotNull(candle)
//        assertEquals(1704070800L, candle?.time)
//        assertEquals(0.13328890993840303, candle!!.open, 1e-10)
//        assertEquals(0.13527505585660082, candle.high, 1e-10)
//        assertEquals(0.13328890993840303, candle.low, 1e-10)
//        candle.close?.let { assertEquals(0.13486224761904764, it, 1e-10) }
//        assertEquals(8307.623372, candle.volume, 1e-10)
//    }

    // TODO, pull saved Price data not Swaps
//    @Test
//    fun makeKnownFifteenCandle() {
//        val testUnit = "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073957696e67526964657273" // Wingriders
//        val fromAsset = Asset.invoke {
//            unit = testUnit
//            decimals = 6
//        }
//        val toAsset = Asset.invoke {
//            unit = "lovelace"
//            decimals = 6
//        }
//        val csvString: String = File("src/test/resources/testdata/wingriders/swaps_01Jan24_01Feb24.csv").readText(Charsets.UTF_8)
//        val reader = csvString.byteInputStream().bufferedReader()
//        reader.readLine()
//        val prices: List<Price> = reader.lineSequence()
//            .filter { it.isNotBlank() }
//            .map {
//                val parts = it.split(",")
//                Price {
//                    provider = parts[2].toInt()
//                    asset_id = 1L
//                    quote_asset_id = 2L
//                    price = Helpers.calculatePriceInAsset1(
//                        parts[5].toBigDecimal(),
//                        toAsset.decimals!!,
//                        parts[6].toBigDecimal(),
//                        fromAsset.decimals!!
//                    )
//                    dtg = parts[1].toLong() - Helpers.slotConversionOffset
//                    tx_hash = parts[0]
//                    amount1 = parts[5].toBigDecimal()
//                    amount2 = parts[6].toBigDecimal()
//                    operation = parts[7].toInt()
//                }
//            }
//            .filter { it.dtg in 112500909..112501809 } // Filter to 01 Jan 00:00 to 00:15 ONLY
//            .toList()
//        println("Last price: ${prices.last()}, timestamp: ${LocalDateTime.ofEpochSecond(prices.last().dtg, 0, Helpers.zoneOffset)}")
//        val rawPrices = HistoricalPriceHelpers.transformTradesToPrices(prices, fromAsset, toAsset)
//        println("Last transformed price: ${rawPrices.last()}, last raw price date: ${rawPrices.last().ldt.toEpochSecond(Helpers.zoneOffset)}")
//        println("Raw converted prices, #: ${rawPrices.size}")
//        val candle = HistoricalPriceHelpers.calculateCandleFromSwaps(
//            rawPrices,
//            fromAsset,
//            null,
//            LocalDateTime.ofEpochSecond(1704068100, 0, Helpers.zoneOffset)
//        )
//        assertNotNull(candle)
//        assertEquals(1704068100L, candle?.time)
//        assertEquals(0.13328890993840303, candle!!.open, 1e-10)
//        assertEquals(0.13328890993840303, candle.high, 1e-10)
//        assertEquals(0.13328890993840303, candle.low, 1e-10)
//        candle.close?.let { assertEquals(0.13328890993840303, it, 1e-10) }
//        assertEquals(11.396201, candle.volume, 1e-10)
//    }

    @Test
    fun transformTradesToPrices() {
        val prices = listOf(
            Price {
                provider = 2
                asset_id = 1L
                quote_asset_id = 2L
                price = 0.5
                time = 49400109
                //tx_hash = ""
                amount1 = BigDecimal("100")
                amount2 = BigDecimal("200")
                operation = 0
            },
            Price {
                provider = 2
                asset_id = 1L
                quote_asset_id = 2L
                price = 0.47619047619047616
                time = 49400209
                //tx_hash = ""
                amount1 = BigDecimal("100")
                amount2 = BigDecimal("210")
                operation = 0
            },
            Price {
                provider = 2
                asset_id = 1L
                quote_asset_id = 2L
                price = 0.45454545454545453
                time = 49400309
                //tx_hash = ""
                amount1 = BigDecimal("100")
                amount2 = BigDecimal("220")
                operation = 0
            }
        )
        val fromAsset = Asset.invoke { decimals = 6 }
        val toAsset = Asset.invoke { decimals = 6 }
        val convertedPriceHistory = HistoricalPriceHelpers.transformTradesToPrices(prices, toAsset)
        println("Converted to $convertedPriceHistory")
        assertEquals(3, convertedPriceHistory.size)
        assertEquals(0.5, convertedPriceHistory[0].price!!, 1e-10)
        convertedPriceHistory.forEachIndexed { idx, cph ->
            assertEquals(prices[idx].price, cph.price!!, 1e-10)
            assertEquals(
                prices[idx].amount1.toDouble() / 10.0.pow(toAsset.decimals?.toDouble() ?: 6.0),
                cph.volume,
                1e-10
            )
        }
        assertEquals(
            prices.sumOf { it.amount1.toDouble() / 10.0.pow(toAsset.decimals?.toDouble() ?: 6.0) },
            convertedPriceHistory.sumOf { it.volume },
            1e-10
        )
    }

    @Test
    fun filterOutliers_GrubbsTest() {
        val duration = Duration.ofMinutes(15)
        val rawPrices = mutableListOf<PriceDTO>()
        val outlierFrequency = 25
        for ((count, time) in (1661699696L..1701699696L step 20).withIndex()) {
            if (count % outlierFrequency == 0) {
                rawPrices.add(
                    PriceDTO(
                        LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                        (Math.random() + 30.0),
                        Math.random() * 10000
                    )
                )
            } else {
                rawPrices.add(
                    PriceDTO(
                        LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                        (Math.random() + 1.0),
                        Math.random() * 10000
                    )
                )
            }
        }
        println("# raw prices: ${rawPrices.size}")
        val groupedRawPrices: Map<LocalDateTime, List<PriceDTO>> = rawPrices.groupBy { p -> Helpers.toNearestDiscreteDate(duration, p.ldt) }
        println("grouped raw prices : ${groupedRawPrices.keys.size}")
        val groupedSortedPrices: TreeMap<LocalDateTime, List<PriceDTO>> = TreeMap(groupedRawPrices)
        val expectedRatio = 1 - 1.0 / outlierFrequency
        groupedSortedPrices.forEach { _, groupPrices ->
            println("# prices before filtering, #: ${groupPrices.size}")
            val filteredPrices = HistoricalPriceHelpers.filterOutliersByGrubbsTest(groupPrices, null)
            println("# prices after filtering, #: ${filteredPrices.size}")
            val convertedRatio = filteredPrices.size.toDouble() / groupPrices.size
            println("expected ratio: $expectedRatio vs converted ratio: $convertedRatio")
            val comparison = convertedRatio / expectedRatio
            println("Comparison: $comparison")
            assertTrue(comparison < 1.15 && comparison > 0.85)
        }
    }

    @Test
    fun filterOutliersGrubbsTest() {
        val lastCandle = CandleDTO(
            asset_id = -1L,
            //unit = "1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e",
            quote_asset_id = -2L,
            time = 1679096700,
            open = 0.5211294875189929,
            high = 0.5211294875189929,
            low = 0.5211294875189929,
            close = 0.5211294875189929,
            volume = 4801.687098
        )
        val prices = listOf(
            PriceDTO(price = 1.045992262857252, volume = 978.748416),
            PriceDTO(price = 1.045992262857252, volume = 978.748416),
            PriceDTO(price = 0.5208855320909522, volume = 2025.87217),
            PriceDTO(price = 0.5236808222822782, volume = 490.009618),
            PriceDTO(price = 0.5237673315190541, volume = 99.988559)
        )
        val filteredPrices = HistoricalPriceHelpers.filterOutliersByGrubbsTest(prices, lastCandle)
        println("RawPrices: $prices")
        println("Filtered Prices: $filteredPrices")
    }

    @Test
    fun filterOutliers_ExponentialMovingAverageTest() {
        val now = LocalDateTime.now()
        val data = listOf(
            PriceDTO(now, 1.0, 0.0),
            PriceDTO(now, 2.0, 0.0),
            PriceDTO(now, 3.0, 0.0),
            PriceDTO(now, 4.0, 0.0),
            PriceDTO(now, 5.0, 0.0),
            PriceDTO(now, 6.0, 0.0),
            PriceDTO(now, 7.0, 0.0),
            PriceDTO(now, 8.0, 0.0),
            PriceDTO(now, 9.0, 0.0),
            PriceDTO(now, 10.0, 0.0)
        )
        var ema = data.map { it.price!! }.average()
        var variance = data.map { (it.price!! - ema).pow(2) }.average()
        println("Initial EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        val badValues = mutableListOf<Double>()
        val retainedValues = mutableListOf<Double>()
        for (i in 1..100) {
            val randomNewVal = if (i % 20 == 0) {
                val badValue = (50..100).random().toDouble()
                badValues.add(badValue)
                badValue
            } else {
                (1..10).random().toDouble()
            }
            val filtered = HistoricalPriceHelpers.filterOutliersByEMATest(
                listOf(PriceDTO(now, randomNewVal, 0.0)),
                ema,
                variance
            )
            retainedValues.addAll(filtered.map { it.price!! })
            // Simulate EMA update (since getNextEma is not available)
            val alpha = 0.1
            ema = alpha * randomNewVal + (1 - alpha) * ema
            variance = alpha * (randomNewVal - ema).pow(2) + (1 - alpha) * variance
            println("After value ($randomNewVal): next EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        }
        println("Retained values: $retainedValues")
        badValues.forEach {
            println("Checking no bad value was retained: $it")
            assertTrue(!retainedValues.contains(it))
        }
    }

    @Test
    fun filterOutliers_ExponentialMovingAverageTest_PrimitiveDT() {
        val data = listOf(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
        val movingAverage = data.windowed(3, 1) { it.average() }
        var ema = movingAverage.last()
        var variance = data.map { (it - ema).pow(2) }.average()
        println("Initial EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        val badValues = mutableListOf<Double>()
        val filteredValues = mutableListOf<Double>()
        for (i in 1..100) {
            val randomNewVal = if (i % 20 == 0) {
                val badValue = (50..100).random().toDouble()
                badValues.add(badValue)
                badValue
            } else {
                (1..10).random().toDouble()
            }
            val shouldFilterValue = (ema - randomNewVal).absoluteValue > 2 * sqrt(variance)
            if (shouldFilterValue) filteredValues.add(randomNewVal)
            println("Should filter (val: $randomNewVal, ema: $ema, stddev:${sqrt(variance)})?: $shouldFilterValue")
            // Simulate EMA update
            val alpha = 0.1
            ema = alpha * randomNewVal + (1 - alpha) * ema
            variance = alpha * (randomNewVal - ema).pow(2) + (1 - alpha) * variance
            println("After value ($randomNewVal): next EMA: $ema, variance: $variance, stddev: ${sqrt(variance)}")
        }
        println("Filtered values: $filteredValues")
        badValues.forEach {
            println("Checking bad value was identified: $it")
            assertTrue(filteredValues.contains(it))
        }
    }

    @Test
    fun determineClosePrice_NoSlotDuplicates() {
        val filteredGroupPrices = mutableListOf<PriceDTO>()
        for (time in 1661699696L..1701699696L step 100) {
            filteredGroupPrices.add(
                PriceDTO(
                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                    (Math.random() + 20.0),
                    Math.random() * 10000
                )
            )
        }
        val close = HistoricalPriceHelpers.determineClosePrice(filteredGroupPrices)
        assertTrue(close?.equals(filteredGroupPrices.last().price) ?: false)
    }

    @Test
    fun determineClosePrice_WithSlotDuplicates() {
        val filteredGroupPrices = mutableListOf<PriceDTO>()
        for (time in 1661699696L..1701699696L step 100) {
            filteredGroupPrices.add(
                PriceDTO(
                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                    (Math.random() + 20.0),
                    Math.random() * 10000
                )
            )
        }
        val lastPH = filteredGroupPrices.last()
        filteredGroupPrices.add(
            PriceDTO(
                lastPH.ldt,
                lastPH.price?.minus(0.1),
                lastPH.volume
            )
        )
        filteredGroupPrices.add(
            PriceDTO(
                lastPH.ldt,
                lastPH.price?.minus(0.5), // Expect this to be selected (lowest value of last slot group)
                lastPH.volume
            )
        )
        filteredGroupPrices.add(
            PriceDTO(
                lastPH.ldt,
                lastPH.price?.plus(0.5),
                lastPH.volume
            )
        )
        val close = HistoricalPriceHelpers.determineClosePrice(filteredGroupPrices)
        println("determined close: $close, vs expected: ${lastPH.price?.minus(0.5)}")
        assertTrue(close?.equals(lastPH.price?.minus(0.5)) ?: false)
        assertFalse(close?.equals(lastPH.price?.minus(0.1)) ?: false)
        assertFalse(close?.equals(lastPH.price?.plus(0.5)) ?: false)
    }

    @Test
    fun determineOpenPrice() {
        val duration = Duration.ofDays(7)
        val rawPrices = mutableListOf<PriceDTO>()
        for (time in 1671699696L..1701699696L step 10) {
            rawPrices.add(
                PriceDTO(
                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
                    (Math.random() + 20.0),
                    Math.random() * 10000
                )
            )
        }
        val groupedRawPrices: Map<LocalDateTime, List<PriceDTO>> = rawPrices.groupBy { p ->
            Helpers.toNearestDiscreteDate(duration, p.ldt)
        }
        val groupedSortedPrices: TreeMap<LocalDateTime, List<PriceDTO>> = TreeMap(groupedRawPrices)
        groupedSortedPrices.forEach { (dtgKey, groupPrices) ->
            var lastCandle: CandleDTO? = null
            var open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle?.close, dtgKey, null)
            assertNull(open)

            // Case: Null lastCandle, non-null first group price, expect first group price
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle?.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(groupPrices[0].price))

            // Case: Not same dtgKey as lastCandle, non-null lastClose, expect last close
            lastCandle = CandleDTO(-1L, -2L, 1234567, 11.0, 15.0, 8.0, 10.0, 0.0)
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(lastCandle.close))

            // Case: Not same dtgKey as lastCandle, null lastClose, expect first group price
            lastCandle = CandleDTO(-1L, -2L, 1234567, 11.0, 15.0, 8.0, null, 0.0)
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(groupPrices[0].price))

            // Case: Same DTG key, expect retain its open
            lastCandle = CandleDTO(-1L, -2L, dtgKey.toEpochSecond(Helpers.zoneOffset), 11.0, 15.0, 8.0, 11.0, 0.0)
            open = HistoricalPriceHelpers.determineOpenPrice(lastCandle, lastCandle.close, dtgKey, groupPrices[0].price)
            assertNotNull(open)
            assertTrue(open!!.equals(lastCandle.open))
        }
        println("checked open price finder for # candle groups: ${groupedSortedPrices.size}")
    }

//    @Test
//    fun makeRandomCandles() {
//        val testUnit = "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459" // INDY
//        val fromAsset = Asset.invoke { unit = testUnit }
//        val rawPrices = mutableListOf<PriceDTO>()
//        for (time in 1661699696L..1701699696L step 100) {
//            rawPrices.add(
//                PriceDTO(
//                    LocalDateTime.ofEpochSecond(time, 0, Helpers.zoneOffset),
//                    (Math.random() + 1.0),
//                    Math.random() * 10000
//                )
//            )
//        }
//        val candles = HistoricalPriceHelpers.calculateCandleFromSwaps(
//            rawPrices,
//            fromAsset,
//            null,
//            Helpers.toNearestDiscreteDate(Duration.ofDays(7), LocalDateTime.ofEpochSecond(1701699696L, 0, Helpers.zoneOffset))
//        )
//        println("Calculated candles: $candles")
//    }
}