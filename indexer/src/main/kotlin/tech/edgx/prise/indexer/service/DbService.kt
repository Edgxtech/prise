package tech.edgx.prise.indexer.service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import org.ktorm.database.Database
import org.ktorm.database.asIterable
import org.ktorm.dsl.from
import org.ktorm.dsl.map
import org.ktorm.dsl.max
import org.ktorm.dsl.select
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Prices
import java.sql.SQLException
import java.time.Instant

class DbService : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    private val database: Database by inject(named("appDatabase"))

    suspend fun isCaughtUp(): Boolean {
        return withContext(Dispatchers.IO) {
            try {
                val query = database.from(Prices)
                    .select(max(Prices.time))
                log.debug("Generated SQL for isCaughtUp: {}", query.sql)
                val maxTime = query.map { it.getLong(1) }.firstOrNull()
                log.debug("maxTime in DB: {}", maxTime)
                if (maxTime == null) {
                    log.warn("No data in Prices table, assuming not caught up")
                    return@withContext false
                }
                val currentTime = Instant.now().epochSecond
                val timeDiff = currentTime - maxTime
                val isCaughtUp = timeDiff < 300 //5 mins 86_400
                log.debug("isCaughtUp: maxTime=$maxTime, currentTime=$currentTime, timeDiff=$timeDiff, caughtUp=$isCaughtUp")
                isCaughtUp
            } catch (e: Exception) {
                log.error("Failed to check if caught up", e)
                false
            }
        }
    }

    fun getSyncPointTime(): Long? {
        val syncPointTime = database.useConnection { conn ->
            val sql = """
                SELECT MAX(time) AS sync_point_time 
                FROM price 
            """.trimIndent()
            conn.prepareStatement(sql).use { statement ->
                statement.executeQuery().asIterable().map {
                    it.getLong("sync_point_time")
                }
            }
        }.firstOrNull()
        return when (syncPointTime != 0L) {
            true -> syncPointTime
            else -> null
        }
    }

    fun createSchemasIfRequired() {
        createTablesIfNotExist()
        addIndexesIfRequired()
        addPartitionsIfRequired()
    }

    private fun createTablesIfNotExist() {
        val tableCreationSql = mapOf(
            "asset" to """
                CREATE TABLE IF NOT EXISTS asset (
                    id BIGSERIAL PRIMARY KEY,
                    unit VARCHAR(255) NOT NULL UNIQUE,
                    policy VARCHAR(255) NOT NULL,
                    native_name VARCHAR(255) NOT NULL,
                    sidechain VARCHAR(255),
                    decimals INT,
                    logo_uri VARCHAR(255),
                    preferred_name VARCHAR(255),
                    metadata_fetched BOOLEAN,
                    CONSTRAINT idx_asset_unit UNIQUE (unit)
                )
            """.trimIndent(),
            "tx" to """
                CREATE TABLE IF NOT EXISTS tx (
                    id BIGSERIAL PRIMARY KEY,
                    hash BYTEA NOT NULL,
                    CONSTRAINT uk_hash UNIQUE (hash)
                )
            """.trimIndent(),
            "price" to """
                CREATE TABLE IF NOT EXISTS price (
                    asset_id BIGINT NOT NULL,
                    quote_asset_id BIGINT NOT NULL,
                    provider INT NOT NULL,
                    time BIGINT NOT NULL,
                    outlier BOOLEAN DEFAULT NULL,
                    tx_id BIGINT NOT NULL,
                    tx_swap_idx INT NOT NULL,
                    price REAL NOT NULL,
                    amount1 DECIMAL(38,0) NOT NULL,
                    amount2 DECIMAL(38,0) NOT NULL,
                    operation INT NOT NULL,
                    PRIMARY KEY (time, tx_id, tx_swap_idx)
                ) PARTITION BY RANGE (time)
            """.trimIndent(),
            "latest_price" to """
                CREATE TABLE IF NOT EXISTS latest_price (
                    asset_id BIGINT NOT NULL,
                    quote_asset_id BIGINT NOT NULL,
                    provider INT NOT NULL,
                    time BIGINT NOT NULL,
                    tx_id BIGINT NOT NULL,
                    price REAL NOT NULL,
                    amount1 DECIMAL(38,0) NOT NULL,
                    amount2 DECIMAL(38,0) NOT NULL,
                    operation INT NOT NULL,
                    PRIMARY KEY (asset_id, quote_asset_id, provider)
                )
            """.trimIndent()
        )

        tableCreationSql.forEach { (tableName, sql) ->
            try {
                database.useConnection { conn ->
                    conn.prepareStatement(sql).use { statement ->
                        statement.executeUpdate()
                    }
                }
                log.debug("Table $tableName is ready")
            } catch (e: SQLException) {
                log.error("Failed to create table $tableName", e)
                throw e
            }
        }
    }

    fun addIndexesIfRequired() {
        val tableNames = listOf("price", "asset", "tx")
        val indexDefinitions = mapOf(
            "price" to listOf("idx_price_query"),
            "asset" to listOf("idx_asset_unit"),
            "tx" to listOf("uk_hash")
        )

        val tablesWithMissingIndex = tableNames.mapNotNull { tableName ->
            val expectedIndexes = indexDefinitions[tableName] ?: emptyList()
            val missingIndexes = expectedIndexes.filter { expectedIndexName ->
                val indexes = database.useConnection { conn ->
                    val sql = """
                        SELECT indexname
                        FROM pg_indexes
                        WHERE tablename = ? AND schemaname = CURRENT_SCHEMA()
                    """.trimIndent()
                    conn.prepareStatement(sql).use { statement ->
                        statement.setString(1, tableName)
                        statement.executeQuery().asIterable().map {
                            it.getString("indexname")
                        }
                    }
                }
                expectedIndexName !in indexes
            }
            if (missingIndexes.isNotEmpty()) tableName to missingIndexes else null
        }.toMap()

        if (tablesWithMissingIndex.isNotEmpty()) {
            log.info("Adding table indexes for: ${tablesWithMissingIndex.keys}")
        }

        tablesWithMissingIndex.forEach { (tableName, missingIndexes) ->
            missingIndexes.forEach { indexName ->
                val indexSql = when (tableName to indexName) {
                    "price" to "idx_price_query" -> """
                        CREATE INDEX idx_price_query ON price (asset_id, quote_asset_id, provider, time) WHERE outlier IS NULL
                    """.trimIndent()
                    "asset" to "idx_asset_unit" -> """
                        ALTER TABLE asset ADD CONSTRAINT idx_asset_unit UNIQUE (unit)
                    """.trimIndent()
                    "tx" to "uk_hash" -> """
                        ALTER TABLE tx ADD CONSTRAINT uk_hash UNIQUE (hash)
                    """.trimIndent()
                    else -> throw IllegalStateException("No index definition for $tableName.$indexName")
                }
                try {
                    database.useConnection { conn ->
                        conn.prepareStatement(indexSql).use { statement ->
                            statement.executeUpdate()
                        }
                    }
                    log.info("Successfully added index $indexName for table: $tableName")
                } catch (e: SQLException) {
                    log.error("Failed to add index $indexName for $tableName", e)
                    throw e
                }
            }
        }
    }

    fun addPartitionsIfRequired() {
        val tablesToPartition = listOf("price")
        val partitionSql = mapOf(
            "price" to listOf(
                """
                    CREATE TABLE IF NOT EXISTS price_p2022 PARTITION OF price
                    FOR VALUES FROM (MINVALUE) TO (1672531200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2023 PARTITION OF price
                    FOR VALUES FROM (1672531200) TO (1704067200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2024 PARTITION OF price
                    FOR VALUES FROM (1704067200) TO (1735603200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2025 PARTITION OF price
                    FOR VALUES FROM (1735603200) TO (1767139200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2026 PARTITION OF price
                    FOR VALUES FROM (1767139200) TO (1798675200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2027 PARTITION OF price
                    FOR VALUES FROM (1798675200) TO (1830211200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2028 PARTITION OF price
                    FOR VALUES FROM (1830211200) TO (1861747200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2029 PARTITION OF price
                    FOR VALUES FROM (1861747200) TO (1893283200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p2030 PARTITION OF price
                    FOR VALUES FROM (1893283200) TO (1924819200)
                """.trimIndent(),
                """
                    CREATE TABLE IF NOT EXISTS price_p_future PARTITION OF price
                    FOR VALUES FROM (1924819200) TO (MAXVALUE)
                """.trimIndent()
            )
        )

        val tablesNeedingPartitions = tablesToPartition.mapNotNull { tableName ->
            val partitionExists = database.useConnection { conn ->
                val sql = """
                    SELECT part.relname
                    FROM pg_partitioned_table pt
                    JOIN pg_class tbl ON pt.partrelid = tbl.oid
                    JOIN pg_class part ON part.relispartition = true
                    WHERE tbl.relname = ? AND tbl.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = CURRENT_SCHEMA())
                """.trimIndent()
                conn.prepareStatement(sql).use { statement ->
                    statement.setString(1, tableName)
                    val result = statement.executeQuery()
                    result.next() // If any partitions exist, this will return true
                }
            }
            if (!partitionExists) tableName else null
        }

        if (tablesNeedingPartitions.isNotEmpty()) {
            log.info("Adding partitions for tables: $tablesNeedingPartitions")
        }

        tablesNeedingPartitions.forEach { tableName ->
            val sqlStatements = partitionSql[tableName] ?: throw IllegalStateException("No partition SQL defined for $tableName")
            sqlStatements.forEach { sql ->
                try {
                    database.useConnection { conn ->
                        conn.prepareStatement(sql).use { statement ->
                            statement.executeUpdate()
                        }
                    }
                    log.debug("Successfully added partition for: $tableName")
                } catch (e: SQLException) {
                    log.error("Failed to add partition for $tableName with SQL: $sql", e)
                    throw e
                }
            }
        }
    }
}