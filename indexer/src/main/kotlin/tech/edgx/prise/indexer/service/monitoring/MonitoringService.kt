package tech.edgx.prise.indexer.service.monitoring

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import java.net.InetSocketAddress

class MonitoringService(port: Int?) {
    val prometheusMeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    val port = port
    val gaugeCache: MutableMap<String, Double> = mutableMapOf()

    fun incrementCounter(label: String) {
        prometheusMeterRegistry.counter(label).increment(1.0)
    }

    fun setGaugeValue(label: String, value: Double) {
        gaugeCache[label]=value
        prometheusMeterRegistry.gauge("gauge_$label", gaugeCache) { g -> g[label]?: 0.0 }
    }

    fun startServer() {
        val server = HttpServer.create(InetSocketAddress(port?: 9103), 0)
        server.createContext("/metrics", MyHandler(this))
        server.executor = null
        server.start()
        println("Started metrics server on address: ${server.address}")
    }

    internal class MyHandler(monitoringService: MonitoringService) : HttpHandler {
        var monitoringService = monitoringService
        override fun handle(t: HttpExchange) {
            val response = monitoringService.prometheusMeterRegistry.scrape()
            t.sendResponseHeaders(200, response.length.toLong())
            val os = t.responseBody
            os.write(response.toByteArray())
            os.close()
        }
    }
}