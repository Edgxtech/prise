package tech.edgx.prise.webserver.service

import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.CacheEvict
import org.springframework.cache.annotation.Cacheable
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tech.edgx.prise.webserver.domain.Asset
import tech.edgx.prise.webserver.model.tokens.AssetResponse
import tech.edgx.prise.webserver.model.tokens.TopByVolumeRequest
import java.sql.SQLException

interface AssetService {
    fun getAssetIdForUnit(unit: String): Long?
    fun getDistinctAssets(): Set<String>
    fun getAllSupportedPairs(): Set<Pair<String, String>>
    fun getTopByVolume(topByVolumeRequest: TopByVolumeRequest): Set<AssetResponse>
}

@Service("assetService")
@Transactional
class AssetServiceImpl(
    private val jdbcClient: JdbcClient
) : AssetService {

    @Cacheable(cacheNames = ["asset_ids"], key = "#unit")
    @CacheEvict(cacheNames = ["asset_ids"], key = "#unit", condition = "#result == null")
    override fun getAssetIdForUnit(unit: String): Long? {
        return try {
            jdbcClient
                .sql("SELECT id FROM asset WHERE unit = :unit")
                .param("unit", unit)
                .query(Long::class.java)
                .optional()
                .orElse(null)
        } catch (e: SQLException) {
            log.warn("Asset not found: $unit", e)
            null
        } catch (e: Exception) {
            log.error("Error querying asset: $unit", e)
            null
        }
    }

    override fun getAllSupportedPairs(): Set<Pair<String, String>> {
        return try {
            jdbcClient
                .sql("""
                    WITH distinct_pairs AS
                        (SELECT DISTINCT asset_id, quote_asset_id
                        FROM latest_price)
                    SELECT a1.unit AS asset, a2.unit AS quote
                    FROM distinct_pairs dp
                    JOIN asset a1 ON a1.id = dp.asset_id
                    JOIN asset a2 ON a2.id = dp.quote_asset_id
                """.trimIndent())
                .query { rs, _ ->
                    val asset = if (rs.getString("asset") == "lovelace") "ADA" else rs.getString("asset")
                    val quote = if (rs.getString("quote") == "lovelace") "ADA" else rs.getString("quote")
                    asset to quote
                }
                .list()
                .toSet()
        } catch (e: SQLException) {
            log.error("Error querying supported pairs", e)
            emptySet()
        }
    }

    override fun getDistinctAssets(): Set<String> {
        return try {
            jdbcClient
                .sql("""
                        SELECT DISTINCT unit FROM asset where unit != 'lovelace'
                    """.trimIndent())
                .query { rs, _ -> rs.getString("unit") }
                .set()
        } catch (e: SQLException) {
            log.error("Error querying distinct symbols", e)
            emptySet()
        }
    }

    // Query Cache: GET topByVolume::100
    @Cacheable(cacheNames = ["topByVolume"], key = "#topByVolumeRequest.limit")
    override fun getTopByVolume(topByVolumeRequest: TopByVolumeRequest): Set<AssetResponse> {
        return try {
            jdbcClient
                .sql("""
                SELECT
                    a1.unit,
                    a1.native_name,
                    a1.preferred_name,
                    SUM(amount1)/1000000 AS total_volume
                FROM
                    price
                JOIN asset a1 on a1.id = price.asset_id
                WHERE
                    outlier IS NULL
                    AND time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 days')::bigint
                    AND time < EXTRACT(EPOCH FROM NOW())::bigint
                GROUP BY
                    a1.unit,
                    a1.native_name,
                    a1.preferred_name
                ORDER BY
                    total_volume DESC
                LIMIT :limit;
            """.trimIndent())
                .param("limit", topByVolumeRequest.limit)
                .query(AssetResponse::class.java)
                .set()
        } catch (e: SQLException) {
            log.error("Error querying top by volume", e)
            emptySet()
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(AssetServiceImpl::class.java)
    }
}