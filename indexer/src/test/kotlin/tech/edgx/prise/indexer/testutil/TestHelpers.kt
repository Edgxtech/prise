package tech.edgx.prise.indexer.testutil

import tech.edgx.prise.indexer.domain.DexPriceHistoryView
import tech.edgx.prise.indexer.util.Helpers
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream

class TestHelpers {
    companion object {
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
    }
}