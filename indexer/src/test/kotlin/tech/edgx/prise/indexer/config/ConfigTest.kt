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
        assertDoesNotThrow { Configurer.validateProperties(properties) }
    }

    @Test
    fun validateProperties_2() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.withcarp.properties").inputStream()
        properties.load(input)
        properties.setProperty(Configurer.LATEST_PRICES_UPDATE_INTERVAL_PROPERTY, "")
        assertThrows<ConfigurationException> { Configurer.validateProperties(properties) }
    }

    @Test
    fun validateProperties_3() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.withcarp.properties").inputStream()
        properties.load(input)
        properties.setProperty(Configurer.CHAIN_DATABASE_SERVICE_MODULE_PROPERTY, ChainDatabaseServiceEnum.koios.name)
        properties.setProperty(Configurer.KOIOS_DATASOURCE_URL_PROPERTY, "")
        assertThrows<ConfigurationException> { Configurer.validateProperties(properties) }
        properties.setProperty(Configurer.KOIOS_DATASOURCE_APIKEY_PROPERTY, "abcd")
        assertThrows<ConfigurationException> { Configurer.validateProperties(properties) }
        properties.setProperty(Configurer.KOIOS_DATASOURCE_URL_PROPERTY, "koiosurl.com")
        properties.setProperty(Configurer.KOIOS_DATASOURCE_APIKEY_PROPERTY, "")
        assertDoesNotThrow { Configurer.validateProperties(properties) }
    }

    @Test
    fun validateProperties_4() {
        val properties = Properties()
        val input: InputStream = File("src/test/resources/prise.withoutcarp.properties").inputStream()
        properties.load(input)
        assertDoesNotThrow { Configurer.validateProperties(properties) }
    }
}