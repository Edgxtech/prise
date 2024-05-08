package tech.edgx.prise.indexer

import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.runBlocking
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.context.GlobalContext.startKoin
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.koin.dsl.bind
import org.koin.dsl.module
import tech.edgx.prise.indexer.config.Configurer
import tech.edgx.prise.indexer.repository.*
import tech.edgx.prise.indexer.service.AssetService
import tech.edgx.prise.indexer.service.CandleService
import tech.edgx.prise.indexer.service.dataprovider.module.carp.jdbc.CarpJdbcService
import tech.edgx.prise.indexer.service.chain.ChainService
import tech.edgx.prise.indexer.service.classifier.module.MinswapClassifier
import tech.edgx.prise.indexer.service.classifier.module.SundaeswapClassifier
import tech.edgx.prise.indexer.service.classifier.module.WingridersClassifier
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.service.dataprovider.TokenMetadataService
import tech.edgx.prise.indexer.service.dataprovider.common.ChainDatabaseServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.common.TokenMetadataServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.module.koios.KoiosService
import tech.edgx.prise.indexer.service.dataprovider.module.tokenregistry.TokenRegistryService
import tech.edgx.prise.indexer.service.monitoring.MonitoringService
import tech.edgx.prise.indexer.service.price.HistoricalPriceService
import tech.edgx.prise.indexer.service.price.LatestPriceService
import tech.edgx.prise.indexer.thread.LatestPriceBatcher
import tech.edgx.prise.indexer.util.Helpers
import tech.edgx.prise.indexer.util.RunMode
import java.time.LocalDateTime
import java.util.*
import kotlin.system.exitProcess

val priseModules = module {
    single { Configurer(get()) }

    single { MonitoringService(get()) }
    single { AssetRepository(get()) }
    single { AssetService(get()) }
    single { ChainService(get()) }
    single { CarpRepository(get()) }
    single { LatestPriceService(get()) }
    single { BaseCandleRepository(get()) }
    single { CandleService(get()) }
    single { WeeklyCandleRepository(get()) }
    single { DailyCandleRepository(get()) }
    single { HourlyCandleRepository(get()) }
    single { FifteenCandleRepository(get()) }
    single { HistoricalPriceService(get()) }

    /* Choose one ChainDbService */
    single(named(ChainDatabaseServiceEnum.carpJDBC.name)) { CarpJdbcService(get()) } bind ChainDatabaseService::class
    single(named(ChainDatabaseServiceEnum.koios.name)) { KoiosService(get()) } bind ChainDatabaseService::class

    /* Choose one Token metadata service */
    single(named(TokenMetadataServiceEnum.tokenRegistry.name)) { TokenRegistryService() } bind TokenMetadataService::class

    single(named("dexClassifiers")) {
        listOf(WingridersClassifier, SundaeswapClassifier, MinswapClassifier)
    }
}

class PriseRunner(private val args: Array<String>) : KoinComponent {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            startKoin {
                modules(priseModules)
            }
            val runner = PriseRunner(args)
            runner.run()
        }
    }

    fun run() {
        println("Arguments: ${args.joinToString()}")
        val args = Args.parse(args)

        val configurer: Configurer by inject { parametersOf(if (args.hasArg("config")) args.getArg("config") else "prise.properties") }
        val config = configurer.configure()
        println("Config: $config")

        val monitoringService: Optional<MonitoringService> = run {
            if (config.startMetricsServer == true) {
                val monitoringService: MonitoringService by inject{ parametersOf(config.metricsServerPort) }
                Optional.of(monitoringService)
            } else {
                Optional.empty<MonitoringService>()
            }
        }
        if (monitoringService.isPresent) monitoringService.get().startServer()

        val chainService: ChainService by inject { parametersOf(config) }

        val latestPriceIndexerJob = LatestPriceBatcher(config)

        /* Start indexer thread(s) */
        println("Running in mode: ${config.runMode.name} ...")
        when (config.runMode) {
            RunMode.livesync -> {
                /* Start chain sync */
                chainService.startSync { syncStatusCallback ->
                    monitoringService.get().setGaugeValue(Helpers.CHAIN_SYNC_SLOT_LABEL, syncStatusCallback.toDouble())
                }
                println("Started chainsync in livesync")
            }
            RunMode.oneshot -> {
                    /* Start chain sync */
                    chainService.startSync { syncStatusCallback ->
                        val gapUntilSynced = LocalDateTime.now().toEpochSecond(Helpers.zoneOffset) -
                                                syncStatusCallback + Helpers.slotConversionOffset
                        println("Gap(s) until synced: $gapUntilSynced")
                        if (gapUntilSynced < 100) {
                            chainService.stopSync()
                            latestPriceIndexerJob.cancel()
                        }
                        monitoringService.get().setGaugeValue(Helpers.CHAIN_SYNC_SLOT_LABEL, syncStatusCallback.toDouble())
                    }
                println("Started chainsync in oneshot")
            }
        }

        /* Start latest and historical price maker threads */
        runBlocking {
            println("Started latest price updater ...")
            latestPriceIndexerJob.start().join()
        }

        println("Finished ${config.runMode.name} run, to continue synching use run.mode 'live_sync', exiting...")
        (config.appDataSource as HikariDataSource).close()
        (config.carpDataSource as HikariDataSource).close()
        println("bye...")
        exitProcess(0)
    }
}