package tech.edgx.prise.webserver.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.cache.CacheManager
import org.springframework.cache.annotation.EnableCaching
import org.springframework.cache.concurrent.ConcurrentMapCacheManager
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.data.redis.cache.RedisCacheConfiguration
import org.springframework.data.redis.cache.RedisCacheManager
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.RedisStandaloneConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import tech.edgx.prise.webserver.util.ByteArrayRedisSerializer
import java.time.Duration

@Configuration
@EnableCaching
class CachingConfig(
    @Value("\${spring.data.redis.host:localhost}") private val redisHost: String,
    @Value("\${spring.data.redis.port:6379}") private val redisPort: Int
) {
    @Bean
    @Profile("cache")
    fun redisConnectionFactory(): LettuceConnectionFactory {
        return LettuceConnectionFactory(
            RedisStandaloneConfiguration().apply {
                hostName = redisHost
                port = redisPort
            }
        )
    }

    @Bean(name = ["stringRedisTemplate"])
    @Profile("cache")
    fun stringRedisTemplate(connectionFactory: RedisConnectionFactory): StringRedisTemplate {
        return StringRedisTemplate().apply {
            setConnectionFactory(connectionFactory)
        }
    }

    @Bean(name = ["byteArrayRedisTemplate"])
    @Profile("cache")
    fun byteArrayRedisTemplate(connectionFactory: RedisConnectionFactory): RedisTemplate<String, ByteArray> {
        return RedisTemplate<String, ByteArray>().apply {
            setConnectionFactory(connectionFactory)
            keySerializer = StringRedisSerializer()
            valueSerializer = ByteArrayRedisSerializer()
            hashKeySerializer = StringRedisSerializer()
            hashValueSerializer = ByteArrayRedisSerializer()
        }
    }

    @Bean(name = ["redisCacheManager"])
    @Profile("cache")
    fun redisCacheManager(connectionFactory: RedisConnectionFactory): CacheManager {
        val cacheConfig = RedisCacheConfiguration.defaultCacheConfig()
            .serializeKeysWith(RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer()))
            .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(
                JdkSerializationRedisSerializer()
            ))
            .entryTtl(Duration.ofMinutes(5))

        return RedisCacheManager.RedisCacheManagerBuilder
            .fromConnectionFactory(connectionFactory)
            .cacheDefaults(cacheConfig)
            .build()
    }

    @Bean(name = ["noOpCacheManager"])
    @Profile("!cache")
    fun noOpCacheManager(): CacheManager {
        return ConcurrentMapCacheManager()
    }
}