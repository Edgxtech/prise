package tech.edgx.prise.indexer.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.classifier.common.DexClassifierEnum
import tech.edgx.prise.indexer.service.dataprovider.common.ChainDatabaseServiceEnum
import tech.edgx.prise.indexer.service.dataprovider.common.TokenMetadataServiceEnum
import tech.edgx.prise.indexer.util.RunMode
import java.io.File
import java.io.InputStream
import java.util.*
import javax.naming.ConfigurationException

class Configurer(private val configFile: String?): KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass::class.java)

    companion object {

        val RUN_MODE_PROPERTY: String = "run.mode"

        val LATEST_PRICES_UPDATE_INTERVAL_PROPERTY: String = "latest.prices.livesync.update.interval.seconds"

        val MAKE_HISTORICAL_DATA_PROPERTY = "make.historical.data"

        val START_METRICS_SERVER_PROPERTY: String = "start.metrics.server"
        val METRICS_SERVER_PORT_PROPERTY: String = "metrics.server.port"

        val APP_DB_URL_PROPERTY: String = "app.datasource.url"
        val APP_DB_UNAME_PROPERTY: String = "app.datasource.username"
        val APP_DB_PASS_PROPERTY: String = "app.datasource.password"
        val APP_DB_DRIVER_PROPERTY: String = "app.datasource.driver-class-name"

        val TOKEN_METADATA_SERVICE_MODULE_PROPERTY: String = "token.metadata.service.module"
        val CHAIN_DATABASE_SERVICE_MODULE_PROPERTY: String = "chain.database.service.module"

        val CARP_DB_URL_PROPERTY: String = "carp.datasource.url"
        val CARP_DB_UNAME_PROPERTY: String = "carp.datasource.username"
        val CARP_DB_PASS_PROPERTY: String = "carp.datasource.password"
        val CARP_DB_DRIVER_PROPERTY: String = "carp.datasource.driver-class-name"

        val KOIOS_DATASOURCE_URL_PROPERTY: String = "koios.datasource.url"
        val KOIOS_DATASOURCE_APIKEY_PROPERTY: String = "koios.datasource.apikey"

        val CNODE_ADDRESS_PROPERTY: String = "cnode.address"
        val CNODE_PORT_PROPERTY: String = "cnode.port"

        val DEX_CLASSIFIERS_PROPERTY: String = "dex.classifiers"

        val START_POINT_TIME_PROPERTY: String = "start.point.time"

        fun validateProperties(properties: Properties) {
            if (!RunMode.values().map { m -> m.name }.contains(properties.getProperty(RUN_MODE_PROPERTY))) {
                throw ConfigurationException("You selected invalid $RUN_MODE_PROPERTY: ${properties.getProperty(RUN_MODE_PROPERTY)}")
            }
            if (properties.getProperty(LATEST_PRICES_UPDATE_INTERVAL_PROPERTY).toLongOrNull() == null ||
                properties.getProperty(LATEST_PRICES_UPDATE_INTERVAL_PROPERTY).toLong() < 1) {
                throw ConfigurationException("You selected invalid $LATEST_PRICES_UPDATE_INTERVAL_PROPERTY: ${properties.getProperty(LATEST_PRICES_UPDATE_INTERVAL_PROPERTY)}")
            }
            if (properties.getProperty(MAKE_HISTORICAL_DATA_PROPERTY).toBooleanStrictOrNull() == null) {
                throw ConfigurationException("You selected invalid $MAKE_HISTORICAL_DATA_PROPERTY: ${properties.getProperty(MAKE_HISTORICAL_DATA_PROPERTY)}")
            }
            if (properties.getProperty(START_METRICS_SERVER_PROPERTY).toBooleanStrictOrNull() == null) {
                throw ConfigurationException("You selected invalid $START_METRICS_SERVER_PROPERTY: ${properties.getProperty(START_METRICS_SERVER_PROPERTY)}")
            }
            if (properties.getProperty(METRICS_SERVER_PORT_PROPERTY).toIntOrNull() == null ||
                properties.getProperty(LATEST_PRICES_UPDATE_INTERVAL_PROPERTY).toInt() < 0) {
                throw ConfigurationException("You selected invalid $METRICS_SERVER_PORT_PROPERTY: ${properties.getProperty(METRICS_SERVER_PORT_PROPERTY)}")
            }
            if (properties.getProperty(APP_DB_URL_PROPERTY).isNullOrEmpty() ||
                properties.getProperty(APP_DB_DRIVER_PROPERTY).isNullOrEmpty() ||
                properties.getProperty(APP_DB_UNAME_PROPERTY).isNullOrEmpty() ||
                properties.getProperty(APP_DB_PASS_PROPERTY) == null) {
                throw ConfigurationException("A required application db property was not set, check properties for: $APP_DB_URL_PROPERTY, $APP_DB_DRIVER_PROPERTY, $APP_DB_UNAME_PROPERTY and $APP_DB_PASS_PROPERTY")
            }
            if (!TokenMetadataServiceEnum.entries.map { m -> m.name }.contains(properties[TOKEN_METADATA_SERVICE_MODULE_PROPERTY])) {
                throw ConfigurationException("You selected an invalid token metadata service: ${properties.getProperty(TOKEN_METADATA_SERVICE_MODULE_PROPERTY)}")
            }
            if (!ChainDatabaseServiceEnum.entries.map { m -> m.name }.contains(properties[CHAIN_DATABASE_SERVICE_MODULE_PROPERTY])) {
                throw ConfigurationException("You selected an invalid chain database service: ${properties.getProperty(CHAIN_DATABASE_SERVICE_MODULE_PROPERTY)}")
            }
            if ((properties[CHAIN_DATABASE_SERVICE_MODULE_PROPERTY] == ChainDatabaseServiceEnum.carpJDBC.name) &&
                (properties.getProperty(CARP_DB_URL_PROPERTY).isNullOrEmpty() ||
                        properties.getProperty(CARP_DB_DRIVER_PROPERTY).isNullOrEmpty() ||
                        properties.getProperty(CARP_DB_UNAME_PROPERTY).isNullOrEmpty() ||
                        properties.getProperty(CARP_DB_PASS_PROPERTY) == null)) {
                throw ConfigurationException("A required carp db property was not set, check properties for: $CARP_DB_URL_PROPERTY, $CARP_DB_DRIVER_PROPERTY, $CARP_DB_UNAME_PROPERTY and $CARP_DB_PASS_PROPERTY")
            }
            if ((properties[CHAIN_DATABASE_SERVICE_MODULE_PROPERTY] == ChainDatabaseServiceEnum.carpHTTP.name) &&
                properties.getProperty(CARP_DB_URL_PROPERTY).isNullOrEmpty()) {
                throw ConfigurationException("A required carp property was not set, check properties for: $CARP_DB_URL_PROPERTY")
            }
            if ((properties[CHAIN_DATABASE_SERVICE_MODULE_PROPERTY] == ChainDatabaseServiceEnum.koios.name) &&
                properties.getProperty(KOIOS_DATASOURCE_URL_PROPERTY).isNullOrEmpty()) {
                throw ConfigurationException("A required koios property was not set, check properties for: $KOIOS_DATASOURCE_URL_PROPERTY")
            }
            if (properties.getProperty(CHAIN_DATABASE_SERVICE_MODULE_PROPERTY) == ChainDatabaseServiceEnum.koios.name &&
                !properties.getProperty(KOIOS_DATASOURCE_APIKEY_PROPERTY).isNullOrEmpty() &&
                !"""^[A-Za-z0-9_-]{2,}(?:\.[A-Za-z0-9_-]{2,}){2}$""".toRegex().matches(properties.getProperty(KOIOS_DATASOURCE_APIKEY_PROPERTY))) {
                throw ConfigurationException("Koios apikey format was invalid, check properties for: $KOIOS_DATASOURCE_APIKEY_PROPERTY")
            }
            if (properties.getProperty(CNODE_ADDRESS_PROPERTY).isNullOrEmpty()) {
                throw ConfigurationException("A required cardano node property was not set, check properties for: $CNODE_ADDRESS_PROPERTY")
            }
            if (properties.getProperty(CNODE_PORT_PROPERTY).isNullOrEmpty()) {
                throw ConfigurationException("A required cardano node property was not set, check properties for: $CNODE_PORT_PROPERTY")
            }
            if (properties.getProperty(DEX_CLASSIFIERS_PROPERTY).isNullOrEmpty()) {
                throw ConfigurationException("A required property was not set: $DEX_CLASSIFIERS_PROPERTY")
            }
            if (!DexClassifierEnum.entries.map { m -> m.name }.containsAll(properties.getProperty(DEX_CLASSIFIERS_PROPERTY).split(","))) {
                throw ConfigurationException("You selected an invalid dex classifier: ${properties.getProperty(DEX_CLASSIFIERS_PROPERTY)}")
            }
            if ((!properties.getProperty(START_POINT_TIME_PROPERTY).isNullOrEmpty() &&
                        properties.getProperty(START_POINT_TIME_PROPERTY)?.toLongOrNull() == null) ||
                (!properties.getProperty(START_POINT_TIME_PROPERTY).isNullOrEmpty() &&
                        properties.getProperty(START_POINT_TIME_PROPERTY).toLong() < 0L)) {
                throw ConfigurationException("You selected invalid $START_POINT_TIME_PROPERTY: ${properties.getProperty(START_POINT_TIME_PROPERTY)}")
            }
        }
    }

    fun configure(): Config {
        var config = Config()
        val properties = Properties()
        log.info("Using config: ${configFile?: "prise.properties"}")
        val input: InputStream = File(configFile?: "prise.properties").inputStream()
        properties.load(input)
        if (properties.isNullOrEmpty()) throw ConfigurationException("Trouble loading properties, no properties found")
        validateProperties(properties)

        config.runMode = RunMode.valueOf(properties.getProperty(RUN_MODE_PROPERTY))

        config.latestPricesLivesyncUpdateIntervalSeconds = properties.getProperty(LATEST_PRICES_UPDATE_INTERVAL_PROPERTY).toLong()

        config.makeHistoricalData = properties.getProperty(MAKE_HISTORICAL_DATA_PROPERTY).toBoolean()

        config.startMetricsServer = properties.getProperty(START_METRICS_SERVER_PROPERTY).toBoolean()
        config.metricsServerPort = properties.getProperty(METRICS_SERVER_PORT_PROPERTY).toInt()

        config.appDatasourceUrl=properties.getProperty(APP_DB_URL_PROPERTY)
        config.appDatasourceUsername=properties.getProperty(APP_DB_UNAME_PROPERTY)
        config.appDatasourcePassword=properties.getProperty(APP_DB_PASS_PROPERTY)
        config.appDatasourceDriverClassName=properties.getProperty(APP_DB_DRIVER_PROPERTY)

        val appDbConfig = HikariConfig().apply {
            jdbcUrl         = config.appDatasourceUrl
            driverClassName = config.appDatasourceDriverClassName
            username        = config.appDatasourceUsername
            password        = config.appDatasourcePassword
            maximumPoolSize = 10
        }
        config.appDataSource = HikariDataSource(appDbConfig)

        config.chainDatabaseServiceModule=properties.getProperty(CHAIN_DATABASE_SERVICE_MODULE_PROPERTY)

        // TODO, if (config.chainDatabaseServiceModule==ChainDatabaseServiceEnum.carpJDBC.name)
        config.carpDatasourceUrl = properties.getProperty(CARP_DB_URL_PROPERTY)
        config.carpDatasourceUsername = properties.getProperty(CARP_DB_UNAME_PROPERTY)
        config.carpDatasourcePassword = properties.getProperty(CARP_DB_PASS_PROPERTY)
        config.carpDatasourceDriverClassName = properties.getProperty(CARP_DB_DRIVER_PROPERTY)
        val carpDbConfig = HikariConfig().apply {
            jdbcUrl = config.carpDatasourceUrl
            driverClassName = config.carpDatasourceDriverClassName
            username = config.carpDatasourceUsername
            password = config.carpDatasourcePassword
            maximumPoolSize = 10
        }
        config.carpDataSource = HikariDataSource(carpDbConfig)

        config.koiosDatasourceUrl = properties.getProperty(KOIOS_DATASOURCE_URL_PROPERTY)
        config.koiosDatasourceApiKey = properties.getProperty(KOIOS_DATASOURCE_APIKEY_PROPERTY)

        config.startPointTime = properties.getProperty(START_POINT_TIME_PROPERTY)?.toLongOrNull()

        config.cnodeAddress=properties.getProperty(CNODE_ADDRESS_PROPERTY)
        config.cnodePort=properties.getProperty(CNODE_PORT_PROPERTY).toInt()

        config.dexClassifiers = properties.getProperty(DEX_CLASSIFIERS_PROPERTY)
            .split(",")

        validateConfig(config)
        return config
    }

    private fun validateConfig(config: Config) {
        val dexClassifiers: List<DexClassifier> by inject(named("dexClassifiers"))
        if (dexClassifiers.isNullOrEmpty()) {
            throw ConfigurationException("No dex classifiers found, check defined modules")
        }
        if (config.chainDatabaseServiceModule==ChainDatabaseServiceEnum.koios.name &&
            (config.koiosDatasourceUrl.isNullOrEmpty())) {
            throw ConfigurationException("You selected chain db service ${config.chainDatabaseServiceModule} however didnt provide other configs needed")
        }
    }
}