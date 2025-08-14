package tech.edgx.prise.indexer.config

import tech.edgx.prise.indexer.model.RefreshableView
import tech.edgx.prise.indexer.util.RunMode
import javax.sql.DataSource

data class Config(
    var runMode: RunMode = RunMode.livesync,
    var startPointTime: Long? = null,
    var latestPricesLivesyncUpdateIntervalSeconds: Long = 15,
    var appDatasourceUrl: String = "jdbc:mysql://localhost:3306/prise",
    var appDatasourceUsername: String = "<user>",
    var appDatasourcePassword: String = "<pass>",
    var appDatasourceDriverClassName: String = "com.mysql.cj.jdbc.Driver",
    var tokenMetadataServiceModule: String = "tokenRegistry",
    var chainDatabaseServiceModule: String = "yacistore",
    var carpDatasourceUrl: String? = "jdbc:postgresql://localhost:5432/carp_mainnet?currentSchema=public",
    var carpDatasourceUsername: String? = "postgres",
    var carpDatasourcePassword: String? = "<pass>",
    var carpDatasourceDriverClassName: String? = "org.postgresql.Driver",
    var koiosDatasourceUrl: String? = null,
    var koiosDatasourceApiKey: String? = null,
    var blockfrostDatasourceUrl: String? = null,
    var blockfrostDatasourceApiKey: String? = null,
    var yacistoreDatasourceUrl: String? = null,
    var yacistoreDatasourceApiKey: String? = null,
    var dexClassifiers: List<String>? = null,
    var cnodeAddress: String? = null,
    var cnodePort: Int? = null,
    var appDataSource: DataSource? = null,
    var carpDataSource: DataSource? = null,
    var startMetricsServer: Boolean? = true,
    var metricsServerPort: Int? = 9103,
    var logEnv: String = "prod",
    var eventPublishingEnabled: Boolean? = false,
    var messagingType: String? = "redis",
    var messagingHost: String? = "localhost",
    var messagingPort: Int? = 6379,
    var messagingChannel: String? = "prise-events",
    var messagingUsername: String? = null,
    var messagingPassword: String? = null,
    val refreshableViews: Map<String, RefreshableView> = mapOf(
        "candle_fifteen" to RefreshableView(
            cronSchedule = "0 0/15 * * * ?", // Every 15 minutes (00, 15, 30, 45)
            bootstrapCronSchedule = "0 0/2 * * * ?" // Every minute (00, 01, 02, ...)
            //bootstrapCronSchedule = "0/10 * * * * ?" // Every minute (00, 01, 02, ...)
        ),
        "candle_hourly" to RefreshableView(
            cronSchedule = "0 0 * * * ?", // Every hour (00:00, 01:00, ...)
            bootstrapCronSchedule = "0 0/3 * * * ?" // Every 2 minutes (00, 02, 04, ...)
        ),
        "candle_daily" to RefreshableView(
            cronSchedule = "0 0 0 * * ?", // Daily at 00:00
            bootstrapCronSchedule = "0 0/4 * * * ?" // Every 5 minutes (00, 05, 10, ...)
        ),
        "candle_weekly" to RefreshableView(
            cronSchedule = "0 0 0 ? * 1", // Every Sunday at 00:00 (1=Sunday, ? for day-of-month)
            bootstrapCronSchedule = "0 0/5 * * * ?" // Every 15 minutes (00, 15, 30, ...)
        )
    )
) {
    override fun toString(): String {
        val commonConfig = "Config(" +
                "runMode=$runMode, " +
                "startPointTime=$startPointTime, " +
                "latestPricesLivesyncUpdateIntervalSeconds=$latestPricesLivesyncUpdateIntervalSeconds, " +
                "appDatasourceUrl=$appDatasourceUrl, " +
                "appDatasourceUsername=$appDatasourceUsername, " +
                "appDatasourcePassword=******, " +
                "appDatasourceDriverClassName=$appDatasourceDriverClassName, " +
                "tokenMetadataServiceModule=$tokenMetadataServiceModule, " +
                "dexClassifiers=$dexClassifiers, " +
                "cnodeAddress=$cnodeAddress, " +
                "cnodePort=$cnodePort, " +
                "appDataSource=$appDataSource, " +
                "startMetricsServer=$startMetricsServer, " +
                "metricsServerPort=$metricsServerPort, " +
                "logEnv=$logEnv, " +
                "eventPublishingEnabled=$eventPublishingEnabled, " +
                "messagingType=$messagingType, " +
                "messagingHost=$messagingHost, " +
                "messagingPort=$messagingPort, " +
                "messagingChannel=$messagingChannel, " +
                "messagingUsername=$messagingUsername, " +
                "messagingPassword=******"

        return when (chainDatabaseServiceModule) {
            "carpJDBC" -> "$commonConfig, " +
                    "chainDatabaseServiceModule=$chainDatabaseServiceModule, " +
                    "(carpDatasourceUrl=$carpDatasourceUrl, " +
                    "carpDatasourceUsername=$carpDatasourceUsername, " +
                    "carpDatasourcePassword=******, " +
                    "carpDatasourceDriverClassName=$carpDatasourceDriverClassName, " +
                    "carpDataSource=$carpDataSource)"
            "koios" -> "$commonConfig, " +
                    "chainDatabaseServiceModule=$chainDatabaseServiceModule, " +
                    "(koiosDatasourceUrl=$koiosDatasourceUrl, " +
                    "koiosDatasourceApiKey=******)"
            "blockfrost" -> "$commonConfig, " +
                    "chainDatabaseServiceModule=$chainDatabaseServiceModule, " +
                    "(blockfrostDatasourceUrl=$blockfrostDatasourceUrl, " +
                    "blockfrostDatasourceApiKey=******)"
            "yacistore" -> "$commonConfig, " +
                    "chainDatabaseServiceModule=$chainDatabaseServiceModule, " +
                    "(yacistoreDatasourceUrl=$yacistoreDatasourceUrl, " +
                    "yacistoreDatasourceApiKey=******)"
            "carpHTTP" -> "$commonConfig, " +
                    "chainDatabaseServiceModule=$chainDatabaseServiceModule"
            else -> "$commonConfig, " +
                    "chainDatabaseServiceModule=$chainDatabaseServiceModule"
        }
    }
}