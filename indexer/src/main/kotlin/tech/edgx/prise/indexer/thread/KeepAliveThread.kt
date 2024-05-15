package tech.edgx.prise.indexer.thread

import com.bloxbean.cardano.yaci.helper.BlockSync
import kotlinx.coroutines.*
import org.koin.core.component.KoinComponent
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.config.Config
import tech.edgx.prise.indexer.util.Helpers
import kotlin.coroutines.CoroutineContext

class KeepAliveThread(config: Config, blockSync: BlockSync) : CoroutineScope, KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    private var job: Job = Job()
    override val coroutineContext: CoroutineContext get() = Dispatchers.Default + job
    private val blockSync: BlockSync = blockSync
    private var config = config

    fun cancel() {
        job.cancel()
    }

    fun start() = launch(CoroutineName("keep_alive_thread")) {
        while (isActive) {
            try {
                Thread.sleep(10000L)
                val randomNo: Int = Helpers.getRandomNumber(0, 60000)
                blockSync.sendKeepAliveMessage(randomNo)
                log.debug("Sent keep alive : $randomNo")
            } catch (e: InterruptedException) {
                log.info("Keep alive thread interrupted")
                break
            }
        }
        println("coroutine done")
    }
}