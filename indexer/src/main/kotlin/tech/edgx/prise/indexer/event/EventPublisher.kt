package tech.edgx.prise.indexer.event

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.lettuce.core.RedisClient
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.model.prices.PriceDTO

interface EventPublisher {
    fun publishPriceEvent(price: PriceDTO)
}

// Used for pushing events across the network via Redis PubSub
// Requires setting: event.publishing.enabled=true
class RedisEventPublisher(
    private val config: Config
) : EventPublisher {
    private val client = RedisClient.create("redis://${config.messagingHost}:${config.messagingPort}")
    private val connection = client.connectPubSub()
    private val log = LoggerFactory.getLogger(javaClass)
    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    override fun publishPriceEvent(price: PriceDTO) {
        try {
            connection.sync().publish(config.messagingChannel!!, objectMapper.writeValueAsString(price))
            log.debug("Published price event: {}, {}, {}", price.time, price.tx_id, price.tx_swap_idx)
        } catch (e: Exception) {
            log.error("Failed to publish swap event", e)
        }
    }

    fun close() {
        connection.close()
        client.shutdown()
    }
}

class NoOpEventPublisher : EventPublisher {
    private val log = LoggerFactory.getLogger(javaClass)
    override fun publishPriceEvent(price: PriceDTO) {
        // No-op
    }
}