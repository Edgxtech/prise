package tech.edgx.prise.indexer.service

import com.github.benmanes.caffeine.cache.Cache
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import org.ktorm.database.Database
import org.ktorm.dsl.*
import org.ktorm.entity.*
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Asset
import tech.edgx.prise.indexer.domain.Assets

class AssetService : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    private val database: Database by inject(named("appDatabase"))

    private val assetCacheByUnit by inject<Cache<String, Asset>>()
    private val assetCacheById by inject<Cache<Long, Asset>>()

    var batchSize = 500

    val Database.assets get() = this.sequenceOf(Assets)

    fun getAssetsByIds(ids: Set<Long>): Map<Long, Asset> {
        val cachedAssets = assetCacheById.getAllPresent(ids)
        val missingItems = ids.filter { it !in cachedAssets }
        val dbAssets = if (missingItems.isNotEmpty()) {
            getByIds(missingItems).associateBy { it.id }.also { assets ->
                assets.forEach { assetCacheById.put(it.key, it.value) }
            }
        } else {
            emptyMap()
        }
        return cachedAssets + dbAssets
    }

    fun getByIds(ids: List<Long>): List<Asset> {
        return database
            .from(Assets)
            .select()
            .where { Assets.id inList ids }
            .map { Assets.createEntity(it) }
    }

    fun getAssetsByUnits(units: Set<String>): Map<String, Asset> {
        val cachedAssets = assetCacheByUnit.getAllPresent(units)
        val missingUnits = units.filter { it !in cachedAssets }
        val dbAssets = if (missingUnits.isNotEmpty()) {
            getByUnits(missingUnits).associateBy { it.unit }.also { assets ->
                assets.forEach { assetCacheByUnit.put(it.key, it.value) }
            }
        } else {
            emptyMap()
        }
        return cachedAssets + dbAssets
    }

    fun getByUnits(units: List<String>): List<Asset> {
        return database
            .from(Assets)
            .select()
            .where { Assets.unit inList units }
            .map { Assets.createEntity(it) }
    }

    fun update(a: Asset) {
        database.update(Assets) {
            set(it.policy, a.policy)
            set(it.native_name, a.native_name)
            set(it.decimals, a.decimals)
            set(it.sidechain, a.sidechain)
            set(it.logo_uri, a.logo_uri)
            set(it.preferred_name, a.preferred_name)
            set(it.metadata_fetched, a.metadata_fetched)
            where {
                it.unit eq a.unit
            }
        }
    }

    /* for updating / repairing data occasionally */
    fun batchUpdate(assets: List<Asset>) {
        val chunkedAssets = assets.chunked(batchSize)
        chunkedAssets.forEach { ca ->
            database.batchUpdate(Assets) {
                ca.forEach { a ->
                    item {
                        set(it.policy, a.policy)
                        set(it.native_name, a.native_name)
                        set(it.decimals, a.decimals)
                        set(it.sidechain, a.sidechain)
                        set(it.logo_uri, a.logo_uri)
                        set(it.preferred_name, a.preferred_name)
                        set(it.metadata_fetched, a.metadata_fetched)
                        where {
                            it.id eq a.id
                        }
                    }
                }
            }
            log.trace("Batch updated assets")
        }
    }

    fun batchInsert(assets: List<Asset>): Int {
        var total = -1
        assets.chunked(batchSize).forEach { ca ->
           total += database.batchInsert(Assets) {
                ca.forEach { a ->
                    item {
                        set(it.unit, a.unit)
                        set(it.policy, a.policy)
                        set(it.native_name, a.native_name.replace("\u0000", ""))
                        set(it.decimals, a.decimals)
                        set(it.sidechain, a.sidechain)
                        set(it.logo_uri, a.logo_uri)
                        set(it.preferred_name, a.preferred_name)
                        set(it.metadata_fetched, a.metadata_fetched)
                    }
                }
            }.sum()
            log.trace("Batch inserted assets: $total")
        }
        return total
    }

    fun batchInsertOrUpdate(assets: List<Asset>): Int {
        var total = 0
        assets.chunked(batchSize).forEach { ca ->
            database.useTransaction {
                total += database.useConnection { conn ->
                    val sql = """
                        INSERT INTO asset (unit, policy, native_name, decimals, sidechain, logo_uri, preferred_name, metadata_fetched)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (unit) DO UPDATE
                        SET policy = EXCLUDED.policy,
                            native_name = EXCLUDED.native_name,
                            decimals = EXCLUDED.decimals,
                            sidechain = EXCLUDED.sidechain,
                            logo_uri = EXCLUDED.logo_uri,
                            preferred_name = EXCLUDED.preferred_name,
                            metadata_fetched = EXCLUDED.metadata_fetched
                    """.trimIndent()
                    conn.prepareStatement(sql).use { stmt ->
                        ca.forEach { a ->
                            stmt.setString(1, a.unit)
                            stmt.setString(2, a.policy)
                            stmt.setString(3, a.native_name)
                            stmt.setInt(4, a.decimals ?: 0)
                            stmt.setString(5, a.sidechain)
                            stmt.setString(6, a.logo_uri)
                            stmt.setString(7, a.preferred_name)
                            stmt.setBoolean(8, a.metadata_fetched ?: false)
                            stmt.addBatch()
                        }
                        stmt.executeBatch().sum() // Sum of rows affected
                    }
                }
                log.debug("Batch inserted or updated assets: #: $total, units: ${ca.map { it.unit }}")
            }
        }
        log.info("Total assets inserted/updated: $total")
        return total
    }
}