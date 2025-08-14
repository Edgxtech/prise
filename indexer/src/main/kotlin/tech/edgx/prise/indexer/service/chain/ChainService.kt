package tech.edgx.prise.indexer.service.chain

import com.bloxbean.cardano.yaci.core.common.Constants
import com.bloxbean.cardano.yaci.core.common.NetworkType
import com.bloxbean.cardano.yaci.core.model.Block
import com.bloxbean.cardano.yaci.core.model.Era
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Tip
import com.bloxbean.cardano.yaci.helper.BlockSync
import com.bloxbean.cardano.yaci.helper.listener.BlockChainDataListener
import com.bloxbean.cardano.yaci.helper.model.Transaction
import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.parameter.parametersOf
import org.koin.core.qualifier.named
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.event.BlockReceivedEvent
import tech.edgx.prise.indexer.event.EventBus
import tech.edgx.prise.indexer.event.RollbackEvent
import tech.edgx.prise.indexer.service.DbService
import tech.edgx.prise.indexer.service.dataprovider.ChainDatabaseService
import tech.edgx.prise.indexer.util.Helpers
import java.net.Socket
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess
import kotlin.time.toKotlinDuration

class ChainService(private val config: Config) : KoinComponent {
    private val log = LoggerFactory.getLogger(this::class.java)
    private val eventBus: EventBus by inject { parametersOf(config) }
    private val chainDatabaseService: ChainDatabaseService by inject(named(config.chainDatabaseServiceModule)) { parametersOf(config) }
    private val dbService: DbService by inject()
    private lateinit var blockSync: BlockSync
    private var blockLatch = CountDownLatch(1)
    private var rollbackLatch = CountDownLatch(1)
    var initialised = false
    @Volatile
    private var lastBlockReceivedTimeMs = System.currentTimeMillis()
    @Volatile
    private var currentSlot: Long = 0L
    @Volatile
    private var isSynced = false
    private val syncCheckInterval = Duration.ofSeconds(10)
    private val maxBlockInactivityMs = Duration.ofMinutes(3) //300_000 // 5 minutes
    private val keepAliveInterval = Duration.ofSeconds(8)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob().apply {
        invokeOnCompletion { throwable ->
            log.info("Coroutine scope cancelled, cause: $throwable")
        }
    })
    private var keepAliveJob: Job? = null
    private var monitorJob: Job? = null
    private val serviceLatch = CountDownLatch(1)

    data class InitialisationState(
        val chainStartPoint: Point
    )

    fun checkNodeConnection(): Boolean {
        var retryCount = 0
        val maxRetries = 3
        while (retryCount < maxRetries) {
            try {
                Socket(config.cnodeAddress, config.cnodePort!!).use { }
                log.debug("Node connection successful")
                return true
            } catch (e: Exception) {
                retryCount++
                log.warn("Node connection failed (attempt $retryCount/$maxRetries): ${e.message}")
                if (retryCount >= maxRetries) {
                    log.error("Failed to connect to node after $maxRetries attempts")
                    return false
                }
                Thread.sleep(2000) // Wait 2 seconds before retry
            }
        }
        return false
    }

    val blockChainDataListener = object : BlockChainDataListener {
        override fun onBlock(era: Era, block: Block, transactions: List<Transaction>) {
            log.debug("Received block: {}", block.header.headerBody.blockNumber)
            currentSlot = block.header.headerBody.slot
            lastBlockReceivedTimeMs = System.currentTimeMillis()
            val startTime = System.currentTimeMillis()
            try {
                blockLatch = CountDownLatch(1)
                runBlocking {
                    eventBus.publish(BlockReceivedEvent(block))
                }
                log.debug("Awaiting block latch for block {}", block.header.headerBody.blockNumber)
                blockLatch.await()
                val blockNum = block.header.headerBody.blockNumber
                when (isSynced) {
                    true -> log.info("Processed Block >> ${blockNum}, ${block.header.headerBody.blockHash}, ${block.header.headerBody.slot}")
                    false -> {
                        if (blockNum % 10 == 0L) {
                            log.info("Processed 10 Block(s) until >> ${blockNum}, " + "${block.header.headerBody.blockHash}, ${block.header.headerBody.slot}")
                        }
                    }
                }
                syncStatusCallback(block.header.headerBody.slot)
                val elapsed = System.currentTimeMillis() - startTime
                if (elapsed > 2000) {
                    log.warn("Processed block ${block.header.headerBody.blockNumber} in ${elapsed}ms")
                }
            } catch (e: Exception) {
                log.error("Failed to process block ${block.header.headerBody.blockNumber}", e)
            } finally {
                blockLatch.countDown()
            }
        }

        override fun onRollback(point: Point?) {
            try {
                if (initialised) {
                    log.info("Rollback called, to point: {}", point)
                    if (point?.slot == 0L || point?.hash == null) {
                        log.info("Ignoring rollback, no point provided")
                        return
                    }
                    rollbackLatch = CountDownLatch(1)
                    runBlocking {
                        log.debug("Stopping block sync")
                        blockSync.stop()
                        eventBus.publish(RollbackEvent(point))
                    }
                    if (!rollbackLatch.await(20, TimeUnit.SECONDS)) {
                        log.error("Rollback processing timed out for point $point")
                        rollbackLatch.countDown()
                    }
                } else {
                    initialised = true
                    //log.info("(Re)Initialised...")
                }
            } catch (e: Exception) {
                log.error("Failed to rollback to point ${point}", e)
                rollbackLatch.countDown()
            }
        }

        override fun intersactNotFound(tip: Tip) {
            log.debug("Intersect not found for tip: $tip")
            blockSync.stop()
            val rollbackPointTime = tip.point.slot - Helpers.slotConversionOffset
            val syncPointTime = dbService.getSyncPointTime()
            val reInitialisationTime = syncPointTime?.let { minOf(it, rollbackPointTime) } ?: rollbackPointTime
            val rollbackInitialisationState = determineInitialisationState(reInitialisationTime)
            log.info("On intersect not found, re-initialisation time: $reInitialisationTime, state: $rollbackInitialisationState")
            restartBlockSync(rollbackInitialisationState.chainStartPoint, this)
        }
    }

    private var syncStatusCallback: (Long) -> Unit = {}

    fun startSync(syncStatusCallback: (Long) -> Unit) {
        try {
            checkNodeConnection()
            dbService.createSchemasIfRequired()
            val initialisationState = determineInitialisationState(config.startPointTime)
            log.info("Using initialisation state: $initialisationState")
            this.syncStatusCallback = syncStatusCallback
            restartBlockSync(initialisationState.chainStartPoint, blockChainDataListener)
            try {
                serviceLatch.await() // Block main thread until shutdown
                log.debug("Service latch released")
            } catch (e: InterruptedException) {
                log.warn("Service interrupted", e)
                Thread.currentThread().interrupt()
            }
        } catch (e: Exception) {
            log.error("Chain sync service failed", e)
            stopSync()
            exitProcess(1)
        }
    }

    fun determineInitialisationState(providedStartTime: Long?): InitialisationState {
        val startTime = when(providedStartTime != null) {
            true -> {
                if (!::blockSync.isInitialized) log.info("Starting from provided start time: $providedStartTime")
                providedStartTime
            }
            false -> {
                val latestSyncTime = dbService.getSyncPointTime()
                val derivedStartTime = when (latestSyncTime == null) {
                    true -> {
                        log.info("Starting from known dex launch date")
                        Helpers.dexLaunchTime
                    }
                    false -> {
                        log.info("Already have some candles, resuming from last sync point: ${latestSyncTime}, slot: ${latestSyncTime + Helpers.slotConversionOffset}")
                        latestSyncTime
                    }
                }
                if (!::blockSync.isInitialized) log.info("Starting from derived start time: $derivedStartTime")
                derivedStartTime
            }
        }
        val nearestBlock = startTime.plus(Helpers.slotConversionOffset)
            .let { chainDatabaseService.getBlockNearestToSlot(it) }
            ?: throw Exception("Nearest block not found for: $startTime, slot: ${startTime.plus(Helpers.slotConversionOffset)}")
        return InitialisationState(Point(nearestBlock.slot, nearestBlock.hash))
    }

    fun restartBlockSync(startPoint: Point, listener: BlockChainDataListener) {
        try {
            log.info("${ if (::blockSync.isInitialized) "Restarting" else "Starting"} block sync from: {}", startPoint)
            if (!checkNodeConnection()) {
                log.error("Cannot restart block sync: node connection failed")
                return
            }
            stopSync()
            initialised = false
            blockSync = BlockSync(
                config.cnodeAddress,
                config.cnodePort!!,
                NetworkType.MAINNET.protocolMagic,
                Constants.WELL_KNOWN_MAINNET_POINT
            )
            blockSync.startSync(startPoint, listener)
            startChainAssistRoutines()
        } catch (e: Exception) {
            log.error("Failed to start block sync", e)
            stopSync()
            throw e
        }
    }

    private fun startChainAssistRoutines() {
        keepAliveJob = scope.launch {
            log.debug("Keep-alive coroutine started")
            var retryCount = 0
            val maxRetries = 5
            while (isActive) {
                try {
                    val randomNo: Int = Helpers.getRandomNumber(0, 60000)
                    blockSync.sendKeepAliveMessage(randomNo)
                    log.debug("Sent keep alive: {}", randomNo)
                    retryCount = 0 // Reset retry count on success
                    delay(keepAliveInterval.toKotlinDuration())
                } catch (e: CancellationException) {
                    // Happens regularly during chain rollbacks
                    log.debug("Keep-alive coroutine cancelled")
                } catch (e: Exception) {
                    retryCount++
                    if (retryCount >= maxRetries) {
                        log.error("Failed to send keep-alive after $maxRetries attempts, shutting down", e)
                        stopSync()
                        exitProcess(1)
                    } else {
                        log.warn("Error sending keep-alive, retry $retryCount/$maxRetries", e)
                        delay(Duration.ofSeconds(2).toKotlinDuration()) // Wait before retry
                    }
                }
            }
        }
        // Monitor to track sync status, and detect/restart if blocks stop streaming
        monitorJob = scope.launch {
            log.debug("Monitor coroutine started")
            var retryCount = 0
            val maxRetries = 3
            while (isActive) {
                try {
                    val gapSeconds = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - (currentSlot - Helpers.slotConversionOffset)
                    isSynced = gapSeconds < 1000
                    val timeSinceLastBlock = System.currentTimeMillis() - lastBlockReceivedTimeMs
                    log.debug("Sync status: isSynced={}, gapSeconds={}, inactive bsync={}", isSynced, gapSeconds, timeSinceLastBlock > maxBlockInactivityMs.toMillis())
                    if (timeSinceLastBlock > maxBlockInactivityMs.toMillis()) {
                        log.error("No blocks received for ${timeSinceLastBlock / 1000} seconds, restarting block sync (attempt ${retryCount + 1}/$maxRetries)")
                        val initialisationState = determineInitialisationState(dbService.getSyncPointTime())
                        restartBlockSync(initialisationState.chainStartPoint, blockChainDataListener)
                        lastBlockReceivedTimeMs = System.currentTimeMillis()
                        retryCount = 0 // Reset on success
                    } else {
                        log.debug("Time since last block: ${timeSinceLastBlock / 1000} seconds")
                    }
                    delay(syncCheckInterval.toKotlinDuration())
                } catch (e: CancellationException) {
                    log.debug("Monitor coroutine cancelled")
                    throw e
                } catch (e: Exception) {
                    retryCount++
                    log.error("Error checking block activity, retry $retryCount/$maxRetries", e)
                    if (retryCount >= maxRetries) {
                        log.error("Max retries reached, shutting down", e)
                        stopSync()
                        exitProcess(1)
                    }
                    delay(Duration.ofSeconds(5).toKotlinDuration()) // Wait before retry
                }
            }
        }
    }

    fun stopSync() {
        try {
            if (::blockSync.isInitialized) {
                log.debug("Stopping chain sync service")
                blockSync.stop()
                log.info("BlockSync stopped")
                runBlocking {
                    withTimeout(5000) {
                        keepAliveJob?.cancelAndJoin()
                        monitorJob?.cancelAndJoin()
                        log.info("Coroutines terminated")
                    }
                }
                keepAliveJob = null
                monitorJob = null
            }
            serviceLatch.countDown()
        } catch (e: Exception) {
            log.error("Error during stopSync", e)
        }
    }

    fun signalBlockProcessed() {
        blockLatch.countDown()
    }

    fun signalRollbackProcessed() {
        rollbackLatch.countDown()
    }

    fun getIsSynced(): Boolean = isSynced
}