package tech.edgx.prise.webserver.domain

import java.math.BigDecimal
import jakarta.persistence.*
import java.util.*

interface Price {
    val asset_id: Long
    val quote_asset_id: Long
    val provider: Int
    val time: Long
    val tx_id: Long
    val tx_swap_idx: Int
    val price: Float
    val amount1: BigDecimal
    val amount2: BigDecimal
    val operation: Int
    val outlier: Boolean?
}

@Entity
@Table(
    name = "price",
    indexes = [
        Index(
            name = "idx_price_asset_id",
            columnList = "asset_id,quote_asset_id,provider,time"
        )
    ]
)
class PriceImpl(
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "asset_id", nullable = false, foreignKey = ForeignKey(ConstraintMode.NO_CONSTRAINT))
    var asset: AssetImpl? = null,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "quote_asset_id", nullable = false, foreignKey = ForeignKey(ConstraintMode.NO_CONSTRAINT))
    var quote_asset: AssetImpl? = null,

    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "tx_id", nullable = false, foreignKey = ForeignKey(ConstraintMode.NO_CONSTRAINT))
    var tx: TxImpl? = null,

    @Column(name = "provider", nullable = false)
    override var provider: Int = 0,

    @Id
    @Column(name = "time", nullable = false)
    override var time: Long = 0,

    @Id
    @Column(name = "tx_swap_idx", nullable = false)
    override var tx_swap_idx: Int = 0,

    @Column(name = "price", nullable = false)
    override var price: Float = 0.0F,

    @Column(name = "amount1", nullable = false)
    override var amount1: BigDecimal = BigDecimal.ZERO,

    @Column(name = "amount2", nullable = false)
    override var amount2: BigDecimal = BigDecimal.ZERO,

    @Column(name = "operation", nullable = false)
    override var operation: Int = 0,

    @Column(name = "outlier", nullable = true)
    override var outlier: Boolean? = null
) : Price {

    @get:Column(name = "asset_id", insertable = false, updatable = false)
    override val asset_id: Long
        get() = asset?.id ?: throw IllegalStateException("Asset must not be null")

    @get:Column(name = "quote_asset_id", insertable = false, updatable = false)
    override val quote_asset_id: Long
        get() = quote_asset?.id ?: throw IllegalStateException("Quote asset must not be null")

    @get:Column(name = "tx_id", insertable = false, updatable = false)
    override val tx_id: Long
        get() = tx?.id ?: throw IllegalStateException("Transaction must not be null")

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PriceImpl) return false
        return time == other.time &&
                tx_id == other.tx_id &&
                tx_swap_idx == other.tx_swap_idx
    }

    override fun hashCode(): Int {
        return Objects.hash(time, tx_id, tx_swap_idx)
    }
}