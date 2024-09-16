package tech.edgx.prise.indexer.config

import org.ktorm.database.Database
import tech.edgx.prise.indexer.util.RunMode
import javax.sql.DataSource

data class Config(
    var runMode: RunMode = RunMode.livesync,

    var startPointTime: Long? = null,

    var latestPricesLivesyncUpdateIntervalSeconds: Long = 15,

    var makeHistoricalData: Boolean? = null,

    var appDatasourceUrl: String = "jdbc:mysql://localhost:3306/realfi",
    var appDatasourceUsername: String = "broadleaf",
    var appDatasourcePassword: String = "broadleaf",
    var appDatasourceDriverClassName: String = "com.mysql.cj.jdbc.Driver",

    var tokenMetadataServiceModule: String = "tokenRegistry",
    var chainDatabaseServiceModule: String = "carpJDBC",

    var carpDatasourceUrl: String = "jdbc:postgresql://localhost:5432/carp_mainnet?currentSchema=public",
    var carpDatasourceUsername: String = "postgres",
    var carpDatasourcePassword: String = "broadleaf",
    var carpDatasourceDriverClassName: String = "org.postgresql.Driver",

    var koiosDatasourceUrl: String? = null,
    var koiosDatasourceApiKey: String? = null,

    var blockfrostDatasourceUrl: String? = null,
    var blockfrostDatasourceApiKey: String? = null,

    var dexClassifiers: List<String>? = null,

    var cnodeAddress: String? = null,
    var cnodePort: Int? = null,

    var appDataSource: DataSource? = null,
    var carpDataSource: DataSource? = null,

    var startMetricsServer: Boolean? = true,
    var metricsServerPort: Int? = 9103,

    var logEnv: String = "prod",

    var appDatabase: Database? = null,
    var carpDatabase: Database? = null
)