package tech.edgx.prise.indexer.config

import org.slf4j.LoggerFactory
import java.util.Properties

class ConfigHelpers(private val properties: Properties) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Retrieves a nullable configuration value, checking environment variables first, then properties.
     * Returns Pair(value, isFromEnv).
     */
    fun getValue(propKey: String, envKey: String): Pair<String?, Boolean> {
        val envValue = System.getenv(envKey)
        val propValue = properties.getProperty(propKey)
        return if (envValue != null) Pair(envValue, true) else Pair(propValue, false)
    }

    /**
     * Retrieves a non-nullable configuration value with a default if neither environment variable nor property is set.
     * Returns Pair(value, isFromEnv).
     */
    fun getValue(propKey: String, default: String, envKey: String): Pair<String, Boolean> {
        val (value, isFromEnv) = getValue(propKey, envKey)
        return Pair(value ?: default, isFromEnv)
    }

    /**
     * Retrieves a nullable Long value, converting from string if present.
     * Returns Pair(value, isFromEnv).
     */
    fun getLong(propKey: String, envKey: String, default: Long? = null): Pair<Long?, Boolean> {
        val (value, isFromEnv) = getValue(propKey, envKey)
        return Pair(if (value.isNullOrEmpty()) null else value.toLongOrNull() ?: default, isFromEnv)
    }

    /**
     * Retrieves a nullable Int value, converting from string if present.
     * Returns Pair(value, isFromEnv).
     */
    fun getInt(propKey: String, envKey: String, default: Int? = null): Pair<Int?, Boolean> {
        val (value, isFromEnv) = getValue(propKey, envKey)
        return Pair(value?.toIntOrNull() ?: default, isFromEnv)
    }

    /**
     * Retrieves a non-nullable Boolean value with a default.
     * Returns Pair(value, isFromEnv).
     */
    fun getBoolean(propKey: String, envKey: String, default: Boolean): Pair<Boolean, Boolean> {
        val (value, isFromEnv) = getValue(propKey, envKey)
        return Pair(value?.toBooleanStrictOrNull() ?: default, isFromEnv)
    }
}