package tech.edgx.prise.indexer.event

import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import tech.edgx.prise.indexer.domain.Price
import tech.edgx.prise.indexer.model.dex.SwapDTO

sealed class IndexerEvent {
    // Indicates if this event is the last in the block processing series
    open val isFinalBlockEvent: Boolean = false
}
data class BlockReceivedEvent(val block: Block, val startTime: Long = System.currentTimeMillis()) : IndexerEvent()
data class SwapsComputedEvent(val blockSlot: Long, val swaps: List<SwapDTO>) : IndexerEvent()
data class PricesCalculatedEvent(val blockSlot: Long, val prices: List<Price>) : IndexerEvent() {
    override val isFinalBlockEvent: Boolean = true
}
data class RollbackEvent(val point: Point) : IndexerEvent()