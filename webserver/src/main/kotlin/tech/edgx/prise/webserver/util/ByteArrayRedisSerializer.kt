package tech.edgx.prise.webserver.util

import org.springframework.data.redis.serializer.RedisSerializer

class ByteArrayRedisSerializer : RedisSerializer<ByteArray> {
    override fun serialize(value: ByteArray?): ByteArray? = value
    override fun deserialize(bytes: ByteArray?): ByteArray? = bytes
}