package tech.edgx.prise.indexer

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.mockk.mockkClass
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.RegisterExtension
import org.koin.core.context.GlobalContext.startKoin
import org.koin.core.context.GlobalContext.stopKoin
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.dsl.bind
import org.koin.dsl.module
import org.koin.test.KoinTest
import org.koin.test.junit5.mock.MockProviderExtension
import org.ktorm.database.Database
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.config.Configurer
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.event.EventBus
import tech.edgx.prise.indexer.event.EventDispatcher
import tech.edgx.prise.indexer.event.EventPublisher
import tech.edgx.prise.indexer.event.NoOpEventPublisher
import tech.edgx.prise.indexer.event.RedisEventPublisher
import tech.edgx.prise.indexer.processor.PersistenceService
import tech.edgx.prise.indexer.processor.PriceProcessor
import tech.edgx.prise.indexer.processor.SwapProcessor
import tech.edgx.prise.indexer.repository.*
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.DbService
import tech.edgx.prise.indexer.service.PriceService
import tech.edgx.prise.indexer.service.TxService
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.classifier.module.*
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.service.dataprovider.TokenMetadataService
import tech.edgx.prise.indexer.service.dataprovider.common.ChainDatabaseServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.common.TokenMetadataServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.module.blockfrost.BlockfrostService
import tech.edgx.prise.indexer.service.dataprovider.module.carp.jdbc.CarpJdbcService
import tech.edgx.prise.indexer.service.dataprovider.module.koios.KoiosService
import tech.edgx.prise.indexer.service.dataprovider.module.tokenregistry.TokenRegistryService
import tech.edgx.prise.indexer.service.dataprovider.module.yacistore.YaciStoreService
import tech.edgx.prise.indexer.service.monitoring.MonitoringService
import tech.edgx.prise.indexer.thread.MaterialisedViewRefreshWorker
import tech.edgx.prise.indexer.thread.OutlierDetectionWorker
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
open class Base: KoinTest {

    @JvmField
    @RegisterExtension
    val mockProvider = MockProviderExtension.create { clazz ->
        mockkClass(clazz)
    }

    @AfterAll
    fun tearDown() {
        stopKoin()
    }

    @BeforeAll
    fun initialiseBase() {
        startKoin {
            modules(
                module {

                    // Config and Configurer
                    single { (configFile: String?) -> Configurer(configFile) }
                    single<Config> { get<Configurer>(parameters = { parametersOf(getProperty("configFile", "src/test/resources/prise.properties")) }).configure() }

                    single { MonitoringService(get()) }
                    single { AssetService() }
                    single { ChainService(get()) }
                    single { CarpRepository() }
                    single { BaseCandleRepository() }
                    single { TxService() }
                    single { PriceService() }

                    single { DbService() }

                    single { EventBus() }
                    single { SwapProcessor(get()) }
                    single { PriceProcessor(get()) }
                    single { PersistenceService(get()) }
                    single { EventDispatcher(get()) }

                    single { OutlierDetectionWorker() }
                    single { MaterialisedViewRefreshWorker(get()) }

                    single<EventPublisher> { (config: Config) ->
                        if (config.eventPublishingEnabled == true && config.messagingType == "redis") {
                            RedisEventPublisher(config)
                        } else {
                            NoOpEventPublisher()
                        }
                    }

                    single<Cache<String, Asset>> {
                        Caffeine.newBuilder()
                            .expireAfterWrite(10, TimeUnit.MINUTES)
                            .maximumSize(10000)
                            .build()
                    }

                    single(named(ChainDatabaseServiceEnum.carpJDBC.name)) { CarpJdbcService() } bind ChainDatabaseService::class
                    single(named(ChainDatabaseServiceEnum.koios.name)) { KoiosService(get()) } bind ChainDatabaseService::class
                    single(named(ChainDatabaseServiceEnum.blockfrost.name)) { BlockfrostService(get()) } bind ChainDatabaseService::class
                    single(named(ChainDatabaseServiceEnum.yacistore.name)) { YaciStoreService(get()) } bind ChainDatabaseService::class

                    single(named(TokenMetadataServiceEnum.tokenRegistry.name)) { TokenRegistryService() } bind TokenMetadataService::class

                    single(named("dexClassifiers")) {
                        listOf(WingridersClassifier, SundaeswapClassifier, MinswapClassifier, MinswapV2Classifier)
                    }

                    /* For testing, define these individually */
                    single(named("wingridersClassifier")) { WingridersClassifier } bind DexClassifier::class
                    single(named("sundaeswapClassifier")) { SundaeswapClassifier } bind DexClassifier::class
                    single(named("minswapClassifier")) { MinswapClassifier } bind DexClassifier::class
                    single(named("minswapV2Classifier")) { MinswapV2Classifier } bind DexClassifier::class

                    // Database Connections
                    single<Database>(named("appDatabase")) {
                        val config: Config = get()
                        Database.connect(HikariDataSource(HikariConfig().apply {
                            jdbcUrl = config.appDatasourceUrl
                            driverClassName = config.appDatasourceDriverClassName
                            username = config.appDatasourceUsername
                            password = config.appDatasourcePassword
                            maximumPoolSize = 20
                            minimumIdle = 5
                            connectionTimeout = 10000
                            idleTimeout = 600000
                            maxLifetime = 1800000
                            validationTimeout = 5000
                            connectionTestQuery = "SELECT 1"
                            leakDetectionThreshold = 250000 // 250seconds, increased due to initial candle view creation
                        }))
                    }

                    single<Database?>(named("carpDatabase")) {
                        val config: Config = get()
                        if (config.chainDatabaseServiceModule == ChainDatabaseServiceEnum.carpJDBC.name) {
                            Database.connect(HikariDataSource(HikariConfig().apply {
                                jdbcUrl = config.carpDatasourceUrl
                                driverClassName = config.carpDatasourceDriverClassName
                                username = config.carpDatasourceUsername
                                password = config.carpDatasourcePassword
                                maximumPoolSize = 3
                            }))
                        } else {
                            null
                        }
                    }
                })
        }
    }
}