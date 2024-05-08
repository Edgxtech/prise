package tech.edgx.prise.webserver

import org.apache.commons.logging.LogFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

@Component
class StartupListener {
    @Autowired
    lateinit var env: Environment

    @EventListener
    fun handleContextRefresh(event: ContextRefreshedEvent) {
        log.info("Active profiles: ${env.activeProfiles}")
    }

    companion object {
        protected val log = LogFactory.getLog(StartupListener::class.java)
    }
}