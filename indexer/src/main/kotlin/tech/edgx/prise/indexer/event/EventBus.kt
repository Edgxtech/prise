package tech.edgx.prise.indexer.event

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow

class EventBus {
    private val _events = MutableSharedFlow<IndexerEvent>(replay = 0, extraBufferCapacity = 1)
    val events = _events.asSharedFlow()

    suspend fun publish(event: IndexerEvent) {
        _events.emit(event)
    }
}