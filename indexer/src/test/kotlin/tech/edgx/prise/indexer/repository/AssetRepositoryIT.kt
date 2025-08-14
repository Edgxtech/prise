//package tech.edgx.prise.indexer.repository
//
//import org.junit.jupiter.api.Test
//import org.junit.jupiter.api.TestInstance
//import tech.edgx.prise.indexer.domain.Asset
//import org.junit.jupiter.api.Assertions.*
//import org.koin.core.parameter.parametersOf
//import org.koin.test.inject
//import tech.edgx.prise.indexer.Base
//import java.util.*
//
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//class AssetRepositoryIT: Base() {
//
//    //val assetRepository: AssetRepository by inject { parametersOf(config.appDataSource) }
//    val assetRepository: AssetRepository by inject { parametersOf(config.appDatabase) }
//
//    @Test
//    fun batchInsert() {
//        val list1 = listOf(
//            Asset{
//                unit = "test1"
//                //price = 0.1
//                decimals = 6
//            },
//            Asset {
//                unit = "test2"
//                //price = 0.2
//                decimals = 0
//            },
//            Asset {
//                unit = "test3"
//                //price = 0.3
//                decimals = null
//            })
//        list1.forEach { a -> assetRepository.delete(a) } // Cleanup
//        assetRepository.batchInsert(list1)
//        val asset1 = assetRepository.getByUnit(list1[0].unit)
//        val asset2 = assetRepository.getByUnit(list1[1].unit)
//        val asset3 = assetRepository.getByUnit(list1[2].unit)
//        assertTrue(asset1 != null && asset1.decimals==list1[0].decimals)
//        assertTrue(asset2 != null && asset2.decimals==list1[1].decimals)
//        assertTrue(asset3 != null && asset3.decimals==list1[2].decimals)
//        list1.forEach { a -> assetRepository.delete(a) } // Cleanup
//    }
//
//    @Test
//    fun batchUpdate() {
//        val list1 = listOf<Asset>(
//            Asset{
//                unit = "test1"
//                price = 0.1
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test2"
//                price = 0.2
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test3"
//                price = 0.3
//                pricing_provider = "minswap"
//            })
//        val list2 = listOf<Asset>(
//            Asset{
//                unit = "test1"
//                price = 0.11
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test2"
//                price = 0.22
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test3"
//                price = 0.33
//                pricing_provider = "minswap"
//            })
//        list1.forEach { a -> assetRepository.delete(a) } // Cleanup
//        assetRepository.insert(list1[0])
//        assetRepository.insert(list1[1])
//        assetRepository.insert(list1[2])
//        val asset1_rtr_a = assetRepository.getByUnit(list1[0].unit)
//        val asset2_rtr_a = assetRepository.getByUnit(list1[1].unit)
//        val asset3_rtr_a = assetRepository.getByUnit(list1[2].unit)
//        println("Assets before update: ${asset1_rtr_a}, ${asset2_rtr_a}, ${asset3_rtr_a}")
//        assertTrue(asset1_rtr_a != null && asset1_rtr_a.price==list1[0].price)
//        assertTrue(asset2_rtr_a != null && asset2_rtr_a.price==list1[1].price)
//        assertTrue(asset3_rtr_a != null && asset3_rtr_a.price==list1[2].price)
//        assetRepository.batchUpdate(list2)
//        val asset1_rtr_b = assetRepository.getByUnit(list2[0].unit)
//        val asset2_rtr_b = assetRepository.getByUnit(list2[1].unit)
//        val asset3_rtr_b = assetRepository.getByUnit(list2[2].unit)
//        println("Assets after update: ${asset1_rtr_b}, ${asset2_rtr_b}, ${asset3_rtr_b}")
//        assertTrue(asset1_rtr_b != null && asset1_rtr_b.price==list2[0].price)
//        assertTrue(asset2_rtr_b != null && asset2_rtr_b.price==list2[1].price)
//        assertTrue(asset3_rtr_b != null && asset3_rtr_b.price==list2[2].price)
//        list1.forEach { a -> assetRepository.delete(a) } // Cleanup
//    }
//
//    @Test
//    fun batchUpdateSomeOnly() {
//        val list1 = listOf<Asset>(
//            Asset{
//                unit = "test1"
//                price = 0.1
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test2"
//                price = 0.2
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test3"
//                price = 0.3
//                pricing_provider = "minswap"
//            })
//        val list2 = listOf<Asset>(
//            Asset{
//                unit = "test1"
//                price = 0.11
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test2"
//                price = 0.22
//                pricing_provider = "minswap"
//            })
//        list1.forEach { a -> assetRepository.delete(a) } // Cleanup
//        assetRepository.insert(list1[0])
//        assetRepository.insert(list1[1])
//        assetRepository.insert(list1[2])
//        val asset1_rtr_a = assetRepository.getByUnit(list1[0].unit)
//        val asset2_rtr_a = assetRepository.getByUnit(list1[1].unit)
//        val asset3_rtr_a = assetRepository.getByUnit(list1[2].unit)
//        println("Assets before update: ${asset1_rtr_a}, ${asset2_rtr_a}, ${asset3_rtr_a}")
//        assertTrue(asset1_rtr_a != null && asset1_rtr_a.price==list1[0].price)
//        assertTrue(asset2_rtr_a != null && asset2_rtr_a.price==list1[1].price)
//        assertTrue(asset3_rtr_a != null && asset3_rtr_a.price==list1[2].price)
//        assetRepository.batchUpdate(list2)
//        val asset1_rtr_b = assetRepository.getByUnit(list1[0].unit)
//        val asset2_rtr_b = assetRepository.getByUnit(list1[1].unit)
//        val asset3_rtr_b = assetRepository.getByUnit(list1[2].unit)
//        println("Assets after update: ${asset1_rtr_b}, ${asset2_rtr_b}, ${asset3_rtr_b}")
//        assertTrue(asset1_rtr_b != null && asset1_rtr_b.price==list2[0].price)
//        assertTrue(asset2_rtr_b != null && asset2_rtr_b.price==list2[1].price)
//        assertTrue(asset3_rtr_b != null && asset3_rtr_b.price==list1[2].price) // Orig price
//        list1.forEach { a -> assetRepository.delete(a) }
//    }
//
//    @Test
//    fun batchUpdateMissingOriginal() {
//        val list1 = listOf<Asset>(
//            Asset{
//                unit = "test1"
//                price = 0.1
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test2"
//                price = 0.2
//                pricing_provider = "minswap"
//            }
//        )
//        val list2 = listOf<Asset>(
//            Asset{
//                unit = "test1"
//                price = 0.11
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test2"
//                price = 0.22
//                pricing_provider = "minswap"
//            },
//            Asset {
//                unit = "test3"
//                price = 0.33
//                pricing_provider = "minswap"
//            }
//        )
//        list1.forEach { a -> assetRepository.delete(a) } // Cleanup
//        assetRepository.insert(list1[0])
//        assetRepository.insert(list1[1])
//        val asset1_rtr_a = assetRepository.getByUnit(list1[0].unit)
//        val asset2_rtr_a = assetRepository.getByUnit(list1[1].unit)
//        println("Assets before update: ${asset1_rtr_a}, ${asset2_rtr_a}")
//        assertTrue(asset1_rtr_a != null && asset1_rtr_a.price==list1[0].price)
//        assertTrue(asset2_rtr_a != null && asset2_rtr_a.price==list1[1].price)
//        assetRepository.batchUpdate(list2)
//        val asset1_rtr_b = assetRepository.getByUnit(list2[0].unit)
//        val asset2_rtr_b = assetRepository.getByUnit(list2[1].unit)
//        val asset3_rtr_b = assetRepository.getByUnit(list2[2].unit)
//        println("Assets after update: ${asset1_rtr_b}, ${asset2_rtr_b} ${asset3_rtr_b}")
//        assertTrue(asset1_rtr_b != null && asset1_rtr_b.price==list2[0].price)
//        assertTrue(asset2_rtr_b != null && asset2_rtr_b.price==list2[1].price)
//        assertTrue(asset3_rtr_b == null)
//        list1.forEach { a -> assetRepository.delete(a) }
//    }
//}