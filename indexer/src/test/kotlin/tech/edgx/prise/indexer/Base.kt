package tech.edgx.prise.indexer

import com.zaxxer.hikari.HikariDataSource
import io.mockk.mockkClass
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.RegisterExtension
import org.koin.core.context.GlobalContext.startKoin
import org.koin.core.context.GlobalContext.stopKoin
import org.koin.core.qualifier.named
import org.koin.dsl.bind
import org.koin.dsl.module
import org.koin.test.KoinTest
import org.koin.test.junit5.mock.MockProviderExtension
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.config.Configurer
import tech.edgx.prise.indexer.repository.*
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.CandleService
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.classifier.module.*
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.service.dataprovider.TokenMetadataService
import tech.edgx.prise.indexer.service.dataprovider.common.TokenMetadataServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.module.koios.KoiosService
import tech.edgx.prise.indexer.service.dataprovider.module.tokenregistry.TokenRegistryService
import tech.edgx.prise.indexer.service.monitoring.MonitoringService
import tech.edgx.prise.indexer.service.price.HistoricalPriceService
import tech.edgx.prise.indexer.service.price.LatestPriceService

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
open class Base: KoinTest {

    lateinit var config: Config

    @JvmField
    @RegisterExtension
    val mockProvider = MockProviderExtension.create { clazz ->
        mockkClass(clazz)
    }

    @AfterAll
    fun tearDown() {
        stopKoin()
        println("Closing (App) DB connections..")
        (config.appDataSource as HikariDataSource).close()
    }

    @BeforeAll
    fun initialiseBase() {
        startKoin {
            modules(
                module {
                    single { MonitoringService(get()) }
                    single { AssetRepository(get()) }
                    single { AssetService(get()) }
                    single { ChainService(get()) }
                    single { LatestPriceService(get()) }
                    single { BaseCandleRepository(get()) }
                    single { CandleService(get()) }
                    single { WeeklyCandleRepository(get()) }
                    single { DailyCandleRepository(get()) }
                    single { HourlyCandleRepository(get()) }
                    single { FifteenCandleRepository(get()) }
                    single { HistoricalPriceService(get()) }

                    /* Choose one ChainDbService */
                    single(named("koios")) { KoiosService(get()) } bind ChainDatabaseService::class

                    /* Choose one Token metadata service */
                    single(named(TokenMetadataServiceEnum.tokenRegistry.name)) { TokenRegistryService() } bind TokenMetadataService::class

                    single(named("dexClassifiers")) {
                        listOf(WingridersClassifier, SundaeswapClassifier, MinswapClassifier, MinswapV2Classifier)
                    }

                    /* For testing, define these individually */
                    single(named("wingridersClassifier")) { WingridersClassifier } bind DexClassifier::class
                    single(named("sundaeswapClassifier")) { SundaeswapClassifier } bind DexClassifier::class
                    single(named("minswapClassifier")) { MinswapClassifier } bind DexClassifier::class
                    single(named("minswapV2Classifier")) { MinswapV2Classifier } bind DexClassifier::class
                })
        }
        config = Configurer("src/test/resources/prise.withoutcarp.properties").configure()
    }
}