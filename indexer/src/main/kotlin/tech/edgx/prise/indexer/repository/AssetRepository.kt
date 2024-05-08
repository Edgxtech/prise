package tech.edgx.prise.indexer.repository

import org.koin.core.component.KoinComponent
import org.ktorm.database.Database
import org.ktorm.dsl.*
import org.ktorm.entity.*
import org.ktorm.schema.BaseTable
import org.ktorm.support.mysql.bulkInsert
import org.ktorm.support.mysql.bulkInsertOrUpdate
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.*
import java.time.LocalDateTime
import javax.sql.DataSource

class AssetRepository(dataSource: DataSource): KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass::class.java)
    private val database = Database.connect(dataSource)
    var batch_size = 500

    val Database.assets get() = this.sequenceOf(Assets)

    fun insertOrUpdate(a: Asset) {
        if (getByUnit(a.unit) != null) {
            update(a)
        } else {
            insert(a)
        }
    }

    fun insert(a: Asset) {
        database.assets.add(a)
    }

    fun update(a: Asset) {
        database.update(Assets) {
            set(it.policy, a.policy)
            set(it.native_name, a.native_name)
            set(it.price, a.price)
            set(it.ada_price, a.ada_price)
            set(it.decimals, a.decimals)
            set(it.sidechain, a.sidechain)
            set(it.incomplete_price_data, a.incomplete_price_data)
            set(it.last_price_update, a.last_price_update)
            set(it.logo_uri, a.logo_uri)
            set(it.preferred_name, a.preferred_name)
            set(it.pricing_provider, a.pricing_provider)
            where {
                it.unit eq a.unit and (it.pricing_provider eq a.pricing_provider!!)
            }
        }
    }

    fun delete(asset: Asset) {
        database.assets.removeIf { it.unit eq asset.unit }
    }

    fun Database.truncate(table: BaseTable<*>): Int {
        useConnection { conn ->
            conn.prepareStatement("truncate table ${table.tableName}").use { statement ->
                return statement.executeUpdate()
            }
        }
    }

    fun truncateAllAssets() {
        database.truncate(Assets)
    }

    fun save(asset: Asset) {
        database.update(Assets) {
            set(it.native_name, asset.native_name)
            set(it.unit, asset.unit)
            set(it.price, asset.price)
            set(it.sidechain, asset.sidechain)
            where {
                it.id eq asset.id
            }
        }
        asset.flushChanges()
    }

    fun getAllAssets(): List<Asset> {
        return database.assets.toList()
    }

    fun countAssets(): Int {
        return database.assets.count()
    }

    fun getAllCNT(): List<Asset> {
        return database.assets
            .filter { it.unit notEq "lovelace" }
            .toList()
    }

    fun getByUnit(unit: String): Asset? {
        return database.assets.find { it.unit eq unit }
    }

    fun getByUnitAndPricingProvider(unit: String, pricingProvider: String): Asset? {
        return database.assets.find { it.unit eq unit and (it.pricing_provider eq pricingProvider)}
    }

    fun getLastAssetUpdate(): LocalDateTime? {
        return database.assets
            .filter { it.unit notEq "lovelace" }
            .sortedByDescending { it.last_price_update }
            .firstOrNull()
            ?.last_price_update
    }

    /* for normal updating purposes */
    fun batchUpdatePrices(assets: List<Asset>): Int {
        val chunkedAssets: List<List<Asset>> = assets.chunked(batch_size)
        var total = 0
        chunkedAssets.forEach { ca ->
            total += database.batchUpdate(Assets) {
                ca.forEach { a ->
                    item {
                        set(it.price, a.price)
                        set(it.ada_price, a.ada_price)
                        set(it.last_price_update, a.last_price_update)
                        where {
                            (it.unit eq a.unit) and (it.pricing_provider eq a.pricing_provider!!)
                        }
                    }
                }
            }.size
            log.debug("Batch persisted updated assets, result: ${total}" )
        }
        return total
    }

    /* for updating / repairing data occasionally */
    fun batchUpdate(assets: List<Asset>) {
        var chunkedAssets: List<List<Asset>> = assets.chunked(batch_size)
        chunkedAssets.forEach { ca ->
            database.batchUpdate(Assets) {
                ca.forEach { a ->
                    item {
                        set(it.policy, a.policy)
                        set(it.native_name, a.native_name)
                        set(it.price, a.price)
                        set(it.ada_price, a.ada_price)
                        set(it.decimals, a.decimals)
                        set(it.sidechain, a.sidechain)
                        set(it.incomplete_price_data, a.incomplete_price_data)
                        set(it.last_price_update, a.last_price_update)
                        set(it.logo_uri, a.logo_uri)
                        set(it.preferred_name, a.preferred_name)
                        where {
                            (it.unit eq a.unit) and (it.pricing_provider eq a.pricing_provider!!)
                        }
                    }
                }
            }
            log.trace("Batch updated assets" )
        }
    }

    fun batchInsert(assets: List<Asset>): Int {
        val chunkedAssets: List<List<Asset>> = assets.chunked(batch_size)
        var total = 0
        chunkedAssets.forEach { ca ->
            /* Note: can use batchInsert, however since using mysql, can take advantage of bulk insert better perf */
            total += database.bulkInsert(Assets) {
                ca.forEach { a ->
                    //println("Inserting batch: ${a}")
                    item {
                        set(it.unit, a.unit)
                        set(it.policy, a.policy)
                        set(it.native_name, a.native_name)
                        set(it.price, a.price)
                        set(it.ada_price, a.ada_price)
                        set(it.decimals, a.decimals)
                        set(it.sidechain, a.sidechain)
                        set(it.incomplete_price_data, a.incomplete_price_data)
                        set(it.last_price_update, a.last_price_update)
                        set(it.logo_uri, a.logo_uri)
                        set(it.preferred_name, a.preferred_name)
                        set(it.pricing_provider, a.pricing_provider)
                    }
                }
            }
            log.trace("Batch inserted assets: $total" )
        }
        return total
    }

    fun batchInsertOrUpdate(assets: List<Asset>) {
        assets.chunked(batch_size).forEach { ca ->
            ca.forEach { a ->
                database.useTransaction {
                    database.bulkInsertOrUpdate(Assets) {
                        item {
                            set(it.unit, a.unit)
                            set(it.policy, a.policy)
                            set(it.native_name, a.native_name)
                            set(it.price, a.price)
                            set(it.ada_price, a.ada_price)
                            set(it.decimals, a.decimals)
                            set(it.sidechain, a.sidechain)
                            set(it.incomplete_price_data, a.incomplete_price_data)
                            set(it.last_price_update, a.last_price_update)
                            set(it.logo_uri, a.logo_uri)
                            set(it.preferred_name, a.preferred_name)
                            set(it.pricing_provider, a.pricing_provider)
                        }
                        onDuplicateKey {
                            set(it.unit, a.unit)
                            set(it.policy, a.policy)
                            set(it.native_name, a.native_name)
                            set(it.price, a.price)
                            set(it.ada_price, a.ada_price)
                            set(it.decimals, a.decimals)
                            set(it.sidechain, a.sidechain)
                            set(it.incomplete_price_data, a.incomplete_price_data)
                            set(it.last_price_update, a.last_price_update)
                            set(it.logo_uri, a.logo_uri)
                            set(it.preferred_name, a.preferred_name)
                            set(it.pricing_provider, a.pricing_provider)
                        }
                    }
                }
            }
            log.debug ("Batch persisted assets: #: ${assets.size}" )
        }
    }
}