package tech.edgx.prise.webserver.repository

import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.CrudRepository
import tech.edgx.prise.webserver.domain.AssetImpl
import tech.edgx.prise.webserver.domain.AssetView

interface AssetRepository : CrudRepository<AssetImpl?, Long?> {
    @Query(
        value = "select id,price,ada_price,unit,last_price_update,pricing_provider from asset " +
                "where pricing_provider in :pricingProviders " +
                "and sidechain is null " +
                "union all " +
                "select id,price,ada_price,unit,last_price_update,pricing_provider from asset where unit='lovelace'",
        nativeQuery = true
    )
    fun myFindCNT(pricingProviders: List<String>): List<AssetView>?

    @Query(
        value = "select id,price,ada_price,unit,last_price_update,pricing_provider from asset " +
                "where pricing_provider in :pricingProviders " +
                "and sidechain is null " +
                "and unit in :assetUnits",
        nativeQuery = true
    )
    fun myFindSpecifiedCNT(pricingProviders: List<String>, assetUnits: Set<String?>?): List<AssetView>?

    @Query(
        value = "select id,price,ada_price,unit,last_price_update,pricing_provider from asset " +
                "where pricing_provider in :pricingProviders " +
                "and sidechain is null and unit in :assetUnits " +
                "union all " +
                "select id,price,ada_price,unit,last_price_update,pricing_provider from asset where unit='lovelace'",
        nativeQuery = true
    )
    fun myFindSpecifiedCNTAndAda(pricingProviders: List<String>, assetUnits: Set<String?>?): List<AssetView>?
}