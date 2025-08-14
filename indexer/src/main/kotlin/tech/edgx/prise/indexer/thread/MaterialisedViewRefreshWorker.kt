package tech.edgx.prise.indexer.thread

import com.cronutils.model.Cron
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.processor.PersistenceService
import tech.edgx.prise.indexer.service.DbService
import tech.edgx.prise.indexer.service.chain.ChainService
import java.time.*
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.toKotlinDuration

class MaterialisedViewRefreshWorker(private val config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(this::class.java)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val dbService: DbService by inject()
    private val persistenceService: PersistenceService by inject { parametersOf(config) }
    private val viewRefreshSchedules = ConcurrentHashMap<String, CronSchedules>()

    // Cron parser for Quartz-style cron expressions
    private val cronParser = CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))

    data class CronSchedules(
        val cronExecutionTime: ExecutionTime, // For synced mode
        val bootstrapCronExecutionTime: ExecutionTime // For bootstrap mode
    )

    fun start() {
        configureSchedules()
        scheduleViewRefreshes()
        log.info("Materialized view refresh worker started")
    }

    private fun configureSchedules() {
        try {
            config.refreshableViews.forEach { (viewName, viewConfig) ->
                try {
                    val cron: Cron = try {
                        cronParser.parse(viewConfig.cronSchedule)
                    } catch (e: Exception) {
                        throw IllegalArgumentException(
                            "Invalid synced cron schedule for $viewName: ${viewConfig.cronSchedule}", e
                        )
                    }
                    val bootstrapCron: Cron = try {
                        cronParser.parse(viewConfig.bootstrapCronSchedule)
                    } catch (e: Exception) {
                        throw IllegalArgumentException(
                            "Invalid bootstrap cron schedule for $viewName: ${viewConfig.bootstrapCronSchedule}", e
                        )
                    }
                    val cronExecutionTime = ExecutionTime.forCron(cron)
                    val bootstrapCronExecutionTime = ExecutionTime.forCron(bootstrapCron)
                    viewRefreshSchedules[viewName] = CronSchedules(
                        cronExecutionTime,
                        bootstrapCronExecutionTime
                    )
                    log.debug(
                        "Configured schedule for: {}, cron schedule: {}, parsed: {}, bootstrap cron schedule: {}, parsed: {}",
                        viewName, viewConfig.cronSchedule, cron.asString(),
                        viewConfig.bootstrapCronSchedule, bootstrapCron.asString()
                    )
                } catch (e: Exception) {
                    throw IllegalArgumentException(
                        "Failed to configure schedules for $viewName: cron=${viewConfig.cronSchedule}, " +
                                "bootstrap=${viewConfig.bootstrapCronSchedule}", e
                    )
                }
            }
        } catch (e: Exception) {
            log.error("Failed to configure schedules", e)
            throw e
        }
    }

    private fun scheduleViewRefreshes() {
        viewRefreshSchedules.forEach { (viewName, schedules) ->
            scope.launch {
                while (true) {
                    try {
                        //val isSynced = chainService.getIsSynced()
                        val isSynced = dbService.isCaughtUp()
                        val executionTime = if (isSynced) {
                            schedules.cronExecutionTime
                        } else {
                            schedules.bootstrapCronExecutionTime
                        }
                        val cronExpression = if (isSynced) {
                            config.refreshableViews[viewName]?.cronSchedule
                        } else {
                            config.refreshableViews[viewName]?.bootstrapCronSchedule
                        } ?: "unknown"

                        val now = ZonedDateTime.now(ZoneId.of("UTC"))
                        log.debug("Scheduling {}: now={}, cron={}", viewName, now, cronExpression)
                        val nextExecution = executionTime.nextExecution(now)
                            .orElseThrow { IllegalStateException("No next execution for $viewName") }
                        val delayMillis = ChronoUnit.MILLIS.between(now, nextExecution)
                        log.debug("Computed next execution for {}: next={}, delay={}ms", viewName, nextExecution, delayMillis)

                        log.info("Scheduling next refresh for {} at {} (in {}ms) using {} schedule: {}",
                            viewName, nextExecution, delayMillis,
                            if (isSynced) "normal" else "bootstrap", cronExpression
                        )
                        delay(delayMillis)

                        log.debug("Refreshing materialized view: {}", viewName)
                        val startTime = System.currentTimeMillis()
                        persistenceService.refreshView(viewName)
                        val duration = System.currentTimeMillis() - startTime
                        log.info("Refreshed {} view due to scheduling in {}ms", viewName, duration)
                    } catch (e: Exception) {
                        log.error("Error in refresh loop for $viewName", e)
                        delay(Duration.ofMinutes(1).toKotlinDuration())
                    }
                }
            }
        }
    }

    fun stop() {
        log.info("Stopping materialized view refresh worker")
        scope.cancel()
    }
}