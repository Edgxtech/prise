package tech.edgx.prise.indexer.service

import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.core.qualifier.named
import org.ktorm.database.Database
import org.ktorm.dsl.*
import org.ktorm.entity.*
import org.slf4j.LoggerFactory
import tech.edgx.prise.indexer.domain.Txs
import tech.edgx.prise.indexer.util.Helpers

class TxService() : KoinComponent {
    private val log = LoggerFactory.getLogger(javaClass)
    private val database: Database by inject(named("appDatabase"))

    val Database.tx get() = this.sequenceOf(Txs)

    /**
     * Retrieves tx_id values for a set of transaction hashes.
     * @param hashes Set of transaction hashes (as ByteArray).
     * @return Map of hex hash (String) to tx_id (Long).
     */
    fun getTxIdsByHashes(hashes: Set<ByteArray>): Map<String, Long> = database.useTransaction {
        database.from(Txs)
            .select(Txs.id, Txs.hash)
            .where { Txs.hash inList hashes }
            .mapNotNull { row ->
                val id = row[Txs.id] ?: return@mapNotNull null // Skip if id is null
                val hash = row[Txs.hash] ?: return@mapNotNull null // Skip if hash is null
                Helpers.binaryToHex(hash) to id
            }
            .toMap()
    }

    /**
     * Batch inserts new transaction hashes and returns tx_id values for all provided hashes.
     * @param hashes List of transaction hashes (as ByteArray).
     * @return Map of hex hash (String) to tx_id (Long).
     */
    fun batchInsertTxs(hashes: List<ByteArray>): Map<String, Long> = database.useTransaction {
        val existing = getTxIdsByHashes(hashes.toSet())
        val newHashes = hashes
            .map { Helpers.binaryToHex(it) to it }
            .filter { it.first !in existing }
            .map { it.second }

        if (newHashes.isNotEmpty()) {
            database.useTransaction {
                database.batchInsert(Txs) {
                    newHashes.forEach { hash ->
                        item {
                            set(Txs.hash, hash)
                        }
                    }
                }
            }
            log.debug("Batch inserted ${newHashes.size} new transaction hashes")
        }

        // Re-query to get all tx_id values, including newly inserted
        getTxIdsByHashes(hashes.toSet())
    }
}