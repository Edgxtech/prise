package tech.edgx.prise.indexer.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.service.classifier.DexClassifier
import tech.edgx.prise.indexer.service.dataprovider.common.ChainDatabaseServiceEnum
import tech.edgx.prise.indexer.util.RunMode
import java.io.File
import java.io.InputStream
import java.util.Properties
import javax.naming.ConfigurationException

class Configurer(private val configFile: String?) : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)

    fun configure(): Config {
        // Load properties
        val properties = Properties()
        log.info("Using config file: ${configFile ?: "prise.properties"}")
        val input: InputStream = File(configFile ?: "prise.properties").inputStream()
        properties.load(input)
        if (properties.isEmpty()) throw ConfigurationException("No properties found in config file")

        // Initialize helpers
        val helpers = ConfigHelpers(properties)
        validateProperties(helpers)

        // Track environment variable overrides
        val envOverrides = mutableListOf<String>()

        // Populate config without database connections
        val config = Config()

        val (runMode, runModeFromEnv) = helpers.getValue(Constants.RUN_MODE_PROPERTY, "livesync", "RUN_MODE")
        if (runModeFromEnv) envOverrides.add("${Constants.RUN_MODE_PROPERTY}=$runMode")
        config.runMode = RunMode.valueOf(runMode)

        val (latestPricesInterval, latestPricesFromEnv) = helpers.getLong(
            Constants.LATEST_PRICES_UPDATE_INTERVAL_PROPERTY, "LATEST_PRICES_LIVESYNC_UPDATE_INTERVAL_SECONDS", 15L
        )
        if (latestPricesFromEnv) envOverrides.add("${Constants.LATEST_PRICES_UPDATE_INTERVAL_PROPERTY}=$latestPricesInterval")
        config.latestPricesLivesyncUpdateIntervalSeconds = latestPricesInterval
            ?: throw ConfigurationException("Invalid ${Constants.LATEST_PRICES_UPDATE_INTERVAL_PROPERTY}")

        val (startMetricsServer, startMetricsFromEnv) = helpers.getBoolean(
            Constants.START_METRICS_SERVER_PROPERTY, "START_METRICS_SERVER", true
        )
        if (startMetricsFromEnv) envOverrides.add("${Constants.START_METRICS_SERVER_PROPERTY}=$startMetricsServer")
        config.startMetricsServer = startMetricsServer

        val (metricsServerPort, metricsPortFromEnv) = helpers.getInt(
            Constants.METRICS_SERVER_PORT_PROPERTY, "METRICS_SERVER_PORT", 9103
        )
        if (metricsPortFromEnv) envOverrides.add("${Constants.METRICS_SERVER_PORT_PROPERTY}=$metricsServerPort")
        config.metricsServerPort = metricsServerPort
            ?: throw ConfigurationException("Invalid ${Constants.METRICS_SERVER_PORT_PROPERTY}")

        val (chainDbServiceModule, chainDbFromEnv) = helpers.getValue(
            Constants.CHAIN_DATABASE_SERVICE_MODULE_PROPERTY, "carpJDBC", "CHAIN_DATABASE_SERVICE_MODULE"
        )
        if (chainDbFromEnv) envOverrides.add("${Constants.CHAIN_DATABASE_SERVICE_MODULE_PROPERTY}=$chainDbServiceModule")
        config.chainDatabaseServiceModule = chainDbServiceModule

        val (tokenMetadataModule, tokenMetadataFromEnv) = helpers.getValue(
            Constants.TOKEN_METADATA_SERVICE_MODULE_PROPERTY, "tokenRegistry", "TOKEN_METADATA_SERVICE_MODULE"
        )
        if (tokenMetadataFromEnv) envOverrides.add("${Constants.TOKEN_METADATA_SERVICE_MODULE_PROPERTY}=$tokenMetadataModule")
        config.tokenMetadataServiceModule = tokenMetadataModule

        val (logEnv, logEnvFromEnv) = helpers.getValue(
            Constants.LOG_ENV_PROPERTY, "prod", "LOG_ENV"
        )
        if (logEnvFromEnv) envOverrides.add("${Constants.LOG_ENV_PROPERTY}=$logEnv")
        config.logEnv = logEnv

        val (appDbUrl, appDbUrlFromEnv) = helpers.getValue(
            Constants.APP_DB_URL_PROPERTY, "jdbc:mysql://localhost:3306/realfi", "APP_DATASOURCE_URL"
        )
        if (appDbUrlFromEnv) envOverrides.add("${Constants.APP_DB_URL_PROPERTY}=$appDbUrl")
        config.appDatasourceUrl = appDbUrl

        val (appDbDriver, appDbDriverFromEnv) = helpers.getValue(
            Constants.APP_DB_DRIVER_PROPERTY, "com.mysql.cj.jdbc.Driver", "APP_DATASOURCE_DRIVER"
        )
        if (appDbDriverFromEnv) envOverrides.add("${Constants.APP_DB_DRIVER_PROPERTY}=$appDbDriver")
        config.appDatasourceDriverClassName = appDbDriver

        val (appDbUsername, appDbUsernameFromEnv) = helpers.getValue(
            Constants.APP_DB_UNAME_PROPERTY, "<user>", "APP_DATASOURCE_USERNAME"
        )
        if (appDbUsernameFromEnv) envOverrides.add("${Constants.APP_DB_UNAME_PROPERTY}=$appDbUsername")
        config.appDatasourceUsername = appDbUsername

        val (appDbPassword, appDbPasswordFromEnv) = helpers.getValue(
            Constants.APP_DB_PASS_PROPERTY, "<pass>", "APP_DATASOURCE_PASSWORD"
        )
        if (appDbPasswordFromEnv) envOverrides.add("${Constants.APP_DB_PASS_PROPERTY}=******")
        config.appDatasourcePassword = appDbPassword

        val (carpDbUrl, carpUrlFromEnv) = helpers.getValue(
            Constants.CARP_DB_URL_PROPERTY, "jdbc:postgresql://localhost:5432/carp_mainnet?currentSchema=public", "CARP_DATASOURCE_URL"
        )
        if (carpUrlFromEnv) envOverrides.add("${Constants.CARP_DB_URL_PROPERTY}=$carpDbUrl")
        config.carpDatasourceUrl = carpDbUrl

        val (carpDbUsername, carpUsernameFromEnv) = helpers.getValue(
            Constants.CARP_DB_UNAME_PROPERTY, "postgres", "CARP_DATASOURCE_USERNAME"
        )
        if (carpUsernameFromEnv) envOverrides.add("${Constants.CARP_DB_UNAME_PROPERTY}=$carpDbUsername")
        config.carpDatasourceUsername = carpDbUsername

        val (carpDbPassword, carpPasswordFromEnv) = helpers.getValue(
            Constants.CARP_DB_PASS_PROPERTY, "<pass>", "CARP_DATASOURCE_PASSWORD"
        )
        if (carpPasswordFromEnv) envOverrides.add("${Constants.CARP_DB_PASS_PROPERTY}=******")
        config.carpDatasourcePassword = carpDbPassword

        val (carpDbDriver, carpDriverFromEnv) = helpers.getValue(
            Constants.CARP_DB_DRIVER_PROPERTY, "org.postgresql.Driver", "CARP_DATASOURCE_DRIVER"
        )
        if (carpDriverFromEnv) envOverrides.add("${Constants.CARP_DB_DRIVER_PROPERTY}=$carpDbDriver")
        config.carpDatasourceDriverClassName = carpDbDriver

        val (koiosUrl, koiosUrlFromEnv) = helpers.getValue(Constants.KOIOS_DATASOURCE_URL_PROPERTY, "KOIOS_DATASOURCE_URL")
        if (koiosUrlFromEnv) envOverrides.add("${Constants.KOIOS_DATASOURCE_URL_PROPERTY}=$koiosUrl")
        config.koiosDatasourceUrl = koiosUrl

        val (koiosApiKey, koiosApiKeyFromEnv) = helpers.getValue(Constants.KOIOS_DATASOURCE_APIKEY_PROPERTY, "KOIOS_DATASOURCE_APIKEY")
        if (koiosApiKeyFromEnv) envOverrides.add("${Constants.KOIOS_DATASOURCE_APIKEY_PROPERTY}=******")
        config.koiosDatasourceApiKey = koiosApiKey

        val (blockfrostUrl, blockfrostUrlFromEnv) = helpers.getValue(Constants.BLOCKFROST_DATASOURCE_URL_PROPERTY, "BLOCKFROST_DATASOURCE_URL")
        if (blockfrostUrlFromEnv) envOverrides.add("${Constants.BLOCKFROST_DATASOURCE_URL_PROPERTY}=$blockfrostUrl")
        config.blockfrostDatasourceUrl = blockfrostUrl

        val (blockfrostApiKey, blockfrostApiKeyFromEnv) = helpers.getValue(Constants.BLOCKFROST_DATASOURCE_APIKEY_PROPERTY, "BLOCKFROST_DATASOURCE_APIKEY")
        if (blockfrostApiKeyFromEnv) envOverrides.add("${Constants.BLOCKFROST_DATASOURCE_APIKEY_PROPERTY}=******")
        config.blockfrostDatasourceApiKey = blockfrostApiKey

        val (yacistoreUrl, yacistoreUrlFromEnv) = helpers.getValue(Constants.YACISTORE_DATASOURCE_URL_PROPERTY, "YACISTORE_DATASOURCE_URL")
        if (yacistoreUrlFromEnv) envOverrides.add("${Constants.YACISTORE_DATASOURCE_URL_PROPERTY}=$yacistoreUrl")
        config.yacistoreDatasourceUrl = yacistoreUrl

        val (yacistoreApiKey, yacistoreApiKeyFromEnv) = helpers.getValue(Constants.YACISTORE_DATASOURCE_APIKEY_PROPERTY, "YACISTORE_DATASOURCE_APIKEY")
        if (yacistoreApiKeyFromEnv) envOverrides.add("${Constants.YACISTORE_DATASOURCE_APIKEY_PROPERTY}=******")
        config.yacistoreDatasourceApiKey = yacistoreApiKey

        val (startPointTime, startPointTimeFromEnv) = helpers.getLong(Constants.START_POINT_TIME_PROPERTY, "START_POINT_TIME")
        if (startPointTimeFromEnv) envOverrides.add("${Constants.START_POINT_TIME_PROPERTY}=${startPointTime ?: "null"}")
        config.startPointTime = startPointTime

        val (cnodeAddress, cnodeAddressFromEnv) = helpers.getValue(Constants.CNODE_ADDRESS_PROPERTY, "CNODE_ADDRESS")
        if (cnodeAddressFromEnv) envOverrides.add("${Constants.CNODE_ADDRESS_PROPERTY}=$cnodeAddress")
        config.cnodeAddress = cnodeAddress

        val (cnodePort, cnodePortFromEnv) = helpers.getInt(Constants.CNODE_PORT_PROPERTY, "CNODE_PORT")
        if (cnodePortFromEnv) envOverrides.add("${Constants.CNODE_PORT_PROPERTY}=$cnodePort")
        config.cnodePort = cnodePort

        val (dexClassifiers, dexClassifiersFromEnv) = helpers.getValue(Constants.DEX_CLASSIFIERS_PROPERTY, "", "DEX_CLASSIFIERS")
        if (dexClassifiersFromEnv) envOverrides.add("${Constants.CNODE_ADDRESS_PROPERTY}=$cnodeAddress")
        config.dexClassifiers = dexClassifiers
            .split(",")
            .filter { it.isNotBlank() }

        val (eventPublishingEnabled, eventPublishingFromEnv) = helpers.getBoolean(
            Constants.EVENT_PUBLISHING_ENABLED_PROPERTY, "EVENT_PUBLISHING_ENABLED", false
        )
        if (eventPublishingFromEnv) envOverrides.add("${Constants.EVENT_PUBLISHING_ENABLED_PROPERTY}=$eventPublishingEnabled")
        config.eventPublishingEnabled = eventPublishingEnabled

        if (config.eventPublishingEnabled == true) {
            val (messagingType, messagingTypeFromEnv) = helpers.getValue(Constants.MESSAGING_TYPE_PROPERTY, "redis", "MESSAGING_TYPE")
            if (messagingTypeFromEnv) envOverrides.add("${Constants.MESSAGING_TYPE_PROPERTY}=$messagingType")
            config.messagingType = messagingType

            val (messagingHost, messagingHostFromEnv) = helpers.getValue(Constants.MESSAGING_HOST_PROPERTY, "localhost", "MESSAGING_HOST")
            if (messagingHostFromEnv) envOverrides.add("${Constants.MESSAGING_HOST_PROPERTY}=$messagingHost")
            config.messagingHost = messagingHost

            val (messagingPort, messagingPortFromEnv) = helpers.getInt(Constants.MESSAGING_PORT_PROPERTY, "MESSAGING_PORT", 6379)
            if (messagingPortFromEnv) envOverrides.add("${Constants.MESSAGING_PORT_PROPERTY}=$messagingPort")
            config.messagingPort = messagingPort

            val (messagingChannel, messagingChannelFromEnv) = helpers.getValue(
                Constants.MESSAGING_CHANNEL_PROPERTY, "prise-events", "MESSAGING_CHANNEL"
            )
            if (messagingChannelFromEnv) envOverrides.add("${Constants.MESSAGING_CHANNEL_PROPERTY}=$messagingChannel")
            config.messagingChannel = messagingChannel

            val (messagingUsername, messagingUsernameFromEnv) = helpers.getValue(
                Constants.MESSAGING_USERNAME_PROPERTY, "MESSAGING_USERNAME"
            )
            if (messagingUsernameFromEnv) envOverrides.add("${Constants.MESSAGING_USERNAME_PROPERTY}=$messagingUsername")
            config.messagingUsername = messagingUsername

            val (messagingPassword, messagingPasswordFromEnv) = helpers.getValue(
                Constants.MESSAGING_PASSWORD_PROPERTY, "MESSAGING_PASSWORD"
            )
            if (messagingPasswordFromEnv) envOverrides.add("${Constants.MESSAGING_PASSWORD_PROPERTY}=******")
            config.messagingPassword = messagingPassword
        }

        // Log environment variable overrides
        if (envOverrides.isNotEmpty()) {
            log.info("Environment variable overrides: [${envOverrides.joinToString(", ")}]")
        } else {
            log.info("No environment variable overrides detected")
        }

        // Validate all properties before connections
        validateConfig(config)

        // Create database connections after validation
        val appDbConfig = HikariConfig().apply {
            jdbcUrl = config.appDatasourceUrl
            driverClassName = config.appDatasourceDriverClassName
            username = config.appDatasourceUsername
            password = config.appDatasourcePassword
            maximumPoolSize = 10
            minimumIdle = 5
            connectionTimeout = 10000
            idleTimeout = 600000
            maxLifetime = 1800000
            validationTimeout = 5000
            connectionTestQuery = "SELECT 1"
            leakDetectionThreshold = 60000
        }
        config.appDataSource = HikariDataSource(appDbConfig)

        if (config.chainDatabaseServiceModule == ChainDatabaseServiceEnum.carpJDBC.name) {
            val carpDbConfig = HikariConfig().apply {
                jdbcUrl = config.carpDatasourceUrl
                driverClassName = config.carpDatasourceDriverClassName
                username = config.carpDatasourceUsername
                password = config.carpDatasourcePassword
                maximumPoolSize = 3
            }
            config.carpDataSource = HikariDataSource(carpDbConfig)
        }

        return config
    }

    private fun validateConfig(config: Config) {
        val dexClassifiers: List<DexClassifier> by inject(named("dexClassifiers"))
        if (dexClassifiers.isEmpty()) {
            throw ConfigurationException("No dex classifiers found, check defined modules")
        }
        if (config.chainDatabaseServiceModule == ChainDatabaseServiceEnum.koios.name &&
            config.koiosDatasourceUrl.isNullOrEmpty()
        ) {
            throw ConfigurationException("Selected chain db service ${config.chainDatabaseServiceModule} but missing required configs")
        }
        if (config.chainDatabaseServiceModule == ChainDatabaseServiceEnum.yacistore.name &&
            config.yacistoreDatasourceUrl.isNullOrEmpty()
        ) {
            throw ConfigurationException("Selected chain db service ${config.chainDatabaseServiceModule} but missing required configs")
        }
        if (config.chainDatabaseServiceModule == ChainDatabaseServiceEnum.carpJDBC.name) {
            if (config.carpDatasourceUrl.isNullOrEmpty()) {
                throw ConfigurationException("Missing ${Constants.CARP_DB_URL_PROPERTY} for carpJDBC")
            }
            if (config.carpDatasourceUsername.isNullOrEmpty()) {
                throw ConfigurationException("Missing ${Constants.CARP_DB_UNAME_PROPERTY} for carpJDBC")
            }
            if (config.carpDatasourcePassword.isNullOrEmpty()) {
                throw ConfigurationException("Missing ${Constants.CARP_DB_PASS_PROPERTY} for carpJDBC")
            }
            if (config.carpDatasourceDriverClassName.isNullOrEmpty()) {
                throw ConfigurationException("Missing ${Constants.CARP_DB_DRIVER_PROPERTY} for carpJDBC")
            }
        }
        if (config.eventPublishingEnabled == true) {
            if (config.messagingType.isNullOrEmpty() ||
                !listOf("redis", "rabbitmq", "kafka").contains(config.messagingType)
            ) {
                throw ConfigurationException("Invalid or missing ${Constants.MESSAGING_TYPE_PROPERTY}: ${config.messagingType}")
            }
            if (config.messagingHost.isNullOrEmpty()) {
                throw ConfigurationException("Missing ${Constants.MESSAGING_HOST_PROPERTY}")
            }
            if (config.messagingPort == null || config.messagingPort!! < 0) {
                throw ConfigurationException("Invalid or missing ${Constants.MESSAGING_PORT_PROPERTY}: ${config.messagingPort}")
            }
            if (config.messagingChannel.isNullOrEmpty()) {
                throw ConfigurationException("Missing ${Constants.MESSAGING_CHANNEL_PROPERTY}")
            }
            if (config.messagingType == "rabbitmq" &&
                (config.messagingUsername.isNullOrEmpty() || config.messagingPassword.isNullOrEmpty())
            ) {
                throw ConfigurationException("Missing ${Constants.MESSAGING_USERNAME_PROPERTY} or ${Constants.MESSAGING_PASSWORD_PROPERTY} for RabbitMQ")
            }
        }
        // Validate app database properties
        if (config.appDatasourceUrl.isEmpty()) {
            throw ConfigurationException("Missing ${Constants.APP_DB_URL_PROPERTY}")
        }
        if (config.appDatasourceUsername.isEmpty()) {
            throw ConfigurationException("Missing ${Constants.APP_DB_UNAME_PROPERTY}")
        }
        if (config.appDatasourcePassword.isEmpty()) {
            throw ConfigurationException("Missing ${Constants.APP_DB_PASS_PROPERTY}")
        }
        if (config.appDatasourceDriverClassName.isEmpty()) {
            throw ConfigurationException("Missing ${Constants.APP_DB_DRIVER_PROPERTY}")
        }
    }

    companion object {
        fun validateProperties(helpers: ConfigHelpers) {
            val (runMode, _) = helpers.getValue(Constants.RUN_MODE_PROPERTY, "livesync", "RUN_MODE")
            if (!RunMode.values().map { it.name }.contains(runMode)) {
                throw ConfigurationException("Invalid ${Constants.RUN_MODE_PROPERTY}: $runMode")
            }
            val (updateInterval, _) = helpers.getLong(
                Constants.LATEST_PRICES_UPDATE_INTERVAL_PROPERTY, "LATEST_PRICES_LIVESYNC_UPDATE_INTERVAL_SECONDS", 15L
            )
            if (updateInterval == null || updateInterval < 1) {
                throw ConfigurationException("Invalid ${Constants.LATEST_PRICES_UPDATE_INTERVAL_PROPERTY}: $updateInterval")
            }
        }
    }
}