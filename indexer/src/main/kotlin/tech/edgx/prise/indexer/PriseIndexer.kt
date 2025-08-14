package tech.edgx.prise.indexer

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.context.GlobalContext.startKoin
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.dsl.bind
import org.koin.dsl.module
import org.ktorm.database.Database
import org.slf4j.LoggerFactory
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
import tech.edgx.prise.indexer.service.*
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.dataprovider.module.carp.jdbc.CarpJdbcService
import tech.edgx.prise.indexer.service.classifier.module.MinswapClassifier
import tech.edgx.prise.indexer.service.classifier.module.MinswapV2Classifier
import tech.edgx.prise.indexer.service.classifier.module.SundaeswapClassifier
import tech.edgx.prise.indexer.service.classifier.module.WingridersClassifier
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.service.dataprovider.TokenMetadataService
import tech.edgx.prise.indexer.service.dataprovider.common.ChainDatabaseServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.common.TokenMetadataServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.module.blockfrost.BlockfrostService
import tech.edgx.prise.indexer.service.dataprovider.module.koios.KoiosService
import tech.edgx.prise.indexer.service.dataprovider.module.tokenregistry.TokenRegistryService
import tech.edgx.prise.indexer.service.dataprovider.module.yacistore.YaciStoreService
import tech.edgx.prise.indexer.service.monitoring.MonitoringService
import tech.edgx.prise.indexer.thread.MaterialisedViewRefreshWorker
import tech.edgx.prise.indexer.thread.OutlierDetectionWorker
import tech.edgx.prise.indexer.util.Helpers
import tech.edgx.prise.indexer.util.RunMode
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit

val priseModules = module {
    // Config and Configurer
    single { (configFile: String?) -> Configurer(configFile) }
    single<Config> { get<Configurer>(parameters = { parametersOf(getProperty("configFile", "prise.properties")) }).configure() }

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
}

class PriseRunner(private val args: Array<String>) : KoinComponent {
    private val log = LoggerFactory.getLogger(this::class.java)

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val runner = PriseRunner(args)
            runner.run()
        }
    }

    fun run() {
        val parsedArgs = Args.parse(args)
        val configFile = if (parsedArgs.hasArg("config")) parsedArgs.getArg("config") else "prise.properties"

        startKoin {
            properties(mapOf("configFile" to (configFile as Any)))
            modules(priseModules)
        }

        val config: Config by inject()
        log.info("Config: $config")

        val monitoringService: Optional<MonitoringService> = if (config.startMetricsServer == true) {
            val monitoringService: MonitoringService by inject { parametersOf(config.metricsServerPort) }
            monitoringService.startServer()
            Optional.of(monitoringService)
        } else {
            Optional.empty()
        }

        val chainService: ChainService by inject { parametersOf(config) }
        val eventDispatcher: EventDispatcher by inject { parametersOf(config) }
        val outlierDetectionWorker: OutlierDetectionWorker by inject()
        val materializedViewRefreshWorker: MaterialisedViewRefreshWorker by inject()

        eventDispatcher.start()
        outlierDetectionWorker.start()
        materializedViewRefreshWorker.start()

        log.info("Running in mode: ${config.runMode.name}")
        when (config.runMode) {
            RunMode.livesync -> {
                chainService.startSync { syncStatusCallback ->
                    log.debug("Synch status callback: {}", syncStatusCallback)
                    monitoringService.ifPresent {
                        it.setGaugeValue(Helpers.CHAIN_SYNC_SLOT_LABEL, syncStatusCallback.toDouble())
                    }
                }
                log.info("Started chainsync in livesync")
            }
            RunMode.oneshot -> {
                chainService.startSync { syncStatusCallback ->
                    val gapUntilSynced = LocalDateTime.now().toEpochSecond(Helpers.zoneOffset) -
                            syncStatusCallback + Helpers.slotConversionOffset
                    log.info("Gap(s) until synced: $gapUntilSynced")
                    if (gapUntilSynced < 100) {
                        log.info("Finished sync, stopping...")
                        chainService.stopSync()
                        log.info("Chain sync stopped")
                        eventDispatcher.stop()
                        outlierDetectionWorker.stop()
                        materializedViewRefreshWorker.stop()
                        shutdown(config)
                    }
                    monitoringService.ifPresent {
                        it.setGaugeValue(Helpers.CHAIN_SYNC_SLOT_LABEL, syncStatusCallback.toDouble())
                    }
                }
                log.info("Started chainsync in oneshot")
            }
        }

        if (config.runMode == RunMode.livesync) {
            // If the ChainService service latch is released
            Runtime.getRuntime().addShutdownHook(Thread {
                log.info("Shutting down...")
                chainService.stopSync()
                log.info("Chain sync stopped")
                eventDispatcher.stop()
                outlierDetectionWorker.stop()
                materializedViewRefreshWorker.stop()
                shutdown(config)
            })
        }
    }

    private fun shutdown(config: Config) {
        log.info("Closing data sources")
        try {
            (config.appDataSource as HikariDataSource).close()
            config.carpDataSource?.let { (it as HikariDataSource).close() }
        } catch (e: Exception) {
            log.error("Error closing data sources", e)
        }
        log.info("Shutdown complete, exiting")
    }
}

