package tech.edgx.prise.indexer.config

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import tech.edgx.prise.indexer.service.dataprovider.common.ChainDatabaseServiceEnum
import java.io.File
import java.io.InputStream
import java.util.*
import javax.naming.ConfigurationException

class ConfigTest {

    @Test
    fun validateProperties_1() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.withcarp.properties").inputStream()
        properties.load(input)
        val helpers = ConfigHelpers(properties)
        assertDoesNotThrow { Configurer.validateProperties(properties, helpers) }
    }

    @Test
    fun validateProperties_2() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.withcarp.properties").inputStream()
        properties.load(input)
        val helpers = ConfigHelpers(properties)
        properties.setProperty(Constants.LATEST_PRICES_UPDATE_INTERVAL_PROPERTY, "")
        assertThrows<ConfigurationException> { Configurer.validateProperties(properties, helpers) }
    }

    @Test
    fun validateProperties_3() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.withcarp.properties").inputStream()
        properties.load(input)
        val helpers = ConfigHelpers(properties)
        properties.setProperty(Constants.CHAIN_DATABASE_SERVICE_MODULE_PROPERTY, ChainDatabaseServiceEnum.koios.name)
        properties.setProperty(Constants.KOIOS_DATASOURCE_URL_PROPERTY, "")
        assertThrows<ConfigurationException> { Configurer.validateProperties(properties, helpers) }
        properties.setProperty(Constants.KOIOS_DATASOURCE_APIKEY_PROPERTY, "abcd")
        assertThrows<ConfigurationException> { Configurer.validateProperties(properties, helpers) }
        properties.setProperty(Constants.KOIOS_DATASOURCE_URL_PROPERTY, "koiosurl.com")
        properties.setProperty(Constants.KOIOS_DATASOURCE_APIKEY_PROPERTY, "")
        assertDoesNotThrow { Configurer.validateProperties(properties, helpers) }
    }

    @Test
    fun validateProperties_4() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.properties").inputStream()
        properties.load(input)
        val helpers = ConfigHelpers(properties)
        assertDoesNotThrow { Configurer.validateProperties(properties, helpers) }
    }

    @Test
    fun validateProperties_5() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.properties").inputStream()
        properties.load(input)
        val helpers = ConfigHelpers(properties)
        properties.setProperty(Constants.CHAIN_DATABASE_SERVICE_MODULE_PROPERTY, ChainDatabaseServiceEnum.yacistore.name)
        properties.setProperty(Constants.YACISTORE_DATASOURCE_URL_PROPERTY, "")
        assertThrows<ConfigurationException> { Configurer.validateProperties(properties, helpers) }
        properties.setProperty(Constants.YACISTORE_DATASOURCE_URL_PROPERTY, "koiosurl.com")
        assertDoesNotThrow { Configurer.validateProperties(properties, helpers) }
    }
}