package tech.edgx.prise.indexer.testutil

import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.google.gson.Gson
import tech.edgx.prise.indexer.domain.DexPriceHistoryView
import tech.edgx.prise.indexer.model.dex.Swap
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.io.PrintWriter

class TestHelpers {
    companion object {
        /*
            TIMESTAMPS:
            01 Jan 24 -
            Epoch timestamp: 1704067200
            Timestamp in seconds: 1704067200
            Date and time (GMT): Monday, 1 January 2024 00:00:00
            SLOT: 1704067200-1591566291=112500909
            Block nearest to slot: 112500909: BlockView(hash=d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8, epoch=458, height=9746375, slot=112500883)

            01 Jan 24: 0100
            Epoch timestamp: 1704070800
            Timestamp in milliseconds: 1704070800000
            Date and time (GMT): Monday, 1 January 2024 01:00:00
            Date and time (your time zone): Monday, 1 January 2024 09:00:00 GMT+08:00
            SLOT:  1704070800-1591566291=112504509

            01 Jan 24: 0010
            Epoch timestamp: 1704067800
            Date and time (GMT): Monday, 1 January 2024 00:10:00
            SLOT:  1704067800-1591566291=112501509
        */

        val point_01Jan24 = Point(112500883, "d1c77b5e2de38cacf6b5ab723fe6681ad879ba3a5405e8e8aa74fa1c73b4a5d8")

        /* block: https://cardanoscan.io/block/10645795 */
        val point_01Aug24 = Point(130904071, "803e98218881f77e98681c4434897026304ce31ad16daa40be8909b90a58f4ed")

        val slot_01Jan24 = 112500909L
        val slot_01Jan24_0100 = 112504509L
        val slot_01Jan24_0005 = 112501209L
        val slot_01Jan24_0010 = 112501509L
        val slot_01Jan24_0020 = 112502109L
        val slot_02Jan24 = 112587309L
        val slot_01Aug24 = 130904109L
        val slot_01Aug24_0010 = 130904709L
        val slot_01Aug24_0100 = 130907709L
        val slot_01Aug24_0030 = 130905909L
        val slot_02Aug24 = 130990509L
        val slot_01Aug24_1200 = 130947309L

        var test_units_a: List<Pair<String,Int?>> = listOf<Pair<String,Int?>>(
            Pair("8e51398904a5d3fc129fbf4f1589701de23c7824d5c90fdb9490e15a434841524c4933", 6),
            Pair("533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459", 6),
            Pair("1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e", 6),
            Pair("4368b0c0c36f8e346fb90f40b1f5e9a360b37035d1e37f32b2d7f558454747534c59", 0),
            Pair("2441ab3351c3b80213a98f4e09ddcf7dabe4879c3c94cc4e7205cb6346495245", 0),
            Pair("9e975c76508686eb2d57985dbaea7e3843767a31b4dcf4e99e5646834d41595a", 6),
            Pair("0b55c812e3a740b8c7219a190753181b097426c14c558ad75d0b48f9474f4f474c45", null)) // Not listed in registry

        var test_units_b: List<String> = listOf<String>(
            "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd43424c50",
            "af2e27f580f7f08e93190a81f72462f153026d06450924726645891b44524950",
            "4623ab311b7d982d8d26fcbe1a9439ca56661aafcdcd8d8a0ef31fd6475245454e53")

        var test_units_c: List<String> = listOf<String>(
            "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b",
            "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0494e4459")

        var test_units_d: List<String> = listOf<String>(
            "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b")

        var test_units_e = listOf("9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d7753554e444145")

        var test_units_f = listOf("1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e776f726c646d6f62696c65746f6b656e")

        fun saveToJson(json: String, fileName: String) {
            File(fileName).writeText(json)
        }

        fun OutputStream.writeDexPriceHistoriesCsv(vals: List<DexPriceHistoryView>?) {
            val writer = bufferedWriter()
            writer.write("unit,amount2,amount1,time,dex")
            writer.newLine()
            vals?.forEach {
                writer.write("${it.policy_id+it.asset_name},${it.amount2},${it.amount1},${it.slot- Helpers.slotConversionOffset},${it.dex}")
                writer.newLine()
            }
            writer.flush()
        }

        fun writeToCsv(vals: List<DexPriceHistoryView>?, filename: String) {
            FileOutputStream(filename).apply { writeDexPriceHistoriesCsv(vals) }
        }

        fun saveComputedSwaps(swaps: List<Swap>) {
            // TEMP, just to speed up devtesting
            val writer: PrintWriter = File("src/test/resources/testdata/sundaeswap/computed_swaps_0000Z01Jan24-0010Z01Jan24.json").printWriter()
            writer.println(Gson().toJson(swaps))
            writer.flush()
            writer.close()
        }
    }
}