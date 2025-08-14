package tech.edgx.prise.webserver.events

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.stereotype.Component
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Profile
import tech.edgx.prise.webserver.model.prices.PriceDTO

@Component
@Profile("cache")
class CacheUpdateListener(
    private val redisConnectionFactory: RedisConnectionFactory,
    @Value("\${messaging.channel:prise-events}") private val channel: String
) {
    private val log = LoggerFactory.getLogger(javaClass::class.java)
    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    @Bean
    @Profile("cache")
    fun redisMessageListener(): RedisMessageListenerContainer {
        return RedisMessageListenerContainer().apply {
            setConnectionFactory(redisConnectionFactory)
            addMessageListener(
                { message, _ ->
                    try {
                        val price = objectMapper.readValue(message.toString(), object : TypeReference<PriceDTO>() {})
                        onNewPrice(price)
                    } catch (e: Exception) {
                        log.error("Failed to parse price event: $message", e)
                    }
                },
                ChannelTopic(channel)
            )
        }
    }

    private fun onNewPrice(price: PriceDTO) {
        log.debug("New price alert: {}", price)
        // Optional, usage e.g. update cache or display event
    }
}