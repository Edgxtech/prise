package tech.edgx.prise.indexer.repository

import org.koin.core.component.KoinComponent
import org.ktorm.database.Database
import org.ktorm.database.asIterable
import tech.edgx.prise.indexer.domain.*
import java.sql.ResultSet
import javax.sql.DataSource

/* Reason for using a jdbc connected repository currently:
   - None of the Ourobourus mini-protocols impl (Ogmios, Yaci) or Blockfrost has a 'spent' UTXO by txHash/idx query. Koios does however is a bit slow
   - Ourobourus mini-protocols do have a 'findIntersection' query, however requires blockhash/slot
   - Could change 'prise' to keep track of latest block point (hash,slot) however since I need 'spent' utxo query anyway,
     instead just implement custom carp db query to findBlockNearestToSlot
   - Could use the standard carp webserver to get the 'spent' transactionOutputs, however cannot also use it to get the blockNearestToSlot
   - Don't want the add another webserver to this project, so instead just connecting by jdbc until finding a simpler approach
 */
class CarpRepository(database: Database): KoinComponent {
    private val database = database

    fun getTransactionOutput(txInHashes: Array<String>, txInIdxs: Array<Int>): List<TransactionOutputView> {
        val transactionOutputs = database.useConnection { conn ->
            val sql = "select \"TransactionOutput\".payload as TXOUT_PAYLOAD, output_index, encode(hash,'hex') as HASH from \"TransactionOutput\" " +
                        "join \"Transaction\" on \"Transaction\".id = \"TransactionOutput\".tx_id " +
                        "where (\"Transaction\".hash, \"TransactionOutput\".output_index) " +
                        "= ANY ( " +
                        "   select decode(hash,'hex'),output_index from " +
                        "       unnest( " +
                        "           ?::text[], " +
                        "           ?::integer[]) " +
                        "       as x(hash,output_index))"
            /* bytea[] for hash here doesn't work not sure why */
            val txInIdsHashesArray = conn.createArrayOf("TEXT", txInHashes)
            val txInIdsIdxArray = conn.createArrayOf("INTEGER", txInIdxs)
            conn.prepareStatement(sql).use { statement ->
                statement.setArray(1, txInIdsHashesArray)
                statement.setArray(2, txInIdsIdxArray)
                statement.executeQuery().asIterable().map {
                    TransactionOutputView(
                        it.getBytes("txout_payload"),
                        it.getInt("output_index"),
                        it.getString("hash"))
                }
            }
        }
        return transactionOutputs
    }

    fun getBlockNearestToSlot(slot: Long): BlockView? {
        val latestBlock = database.useConnection { conn ->
            val sql = "select encode(hash,'hex') as hash, epoch, height, slot from \"Block\" " +
                    "where slot >= ? order by slot asc limit 1"
            conn.prepareStatement(sql).use { statement ->
                statement.setLong(1,slot)
                val rs: ResultSet = statement.executeQuery()
                if (rs.next()) {
                    BlockView(
                        rs.getString("hash"),
                        rs.getInt("epoch"),
                        rs.getLong("height"),
                        rs.getLong("slot"))
                }
                else {
                    return null
                }
            }
        }
        return latestBlock
    }

    fun getLatestBlock(): BlockView? {
        val latestBlock = database.useConnection { conn ->
            val sql = "select encode(hash,'hex') as hash, epoch, height, slot from \"Block\" " +
                    "order by slot desc limit 1"
            conn.prepareStatement(sql).use { statement ->
                val rs: ResultSet = statement.executeQuery()
                if (rs.next()) {
                    BlockView(
                        rs.getString("hash"),
                        rs.getInt("epoch"),
                        rs.getLong("height"),
                        rs.getLong("slot"))
                }
                else {
                    return null
                }
            }
        }
        return latestBlock
    }

    fun getBlockByHeight(height: Long): BlockView? {
        val latestBlock = database.useConnection { conn ->
            val sql = "select encode(hash,'hex') as hash, epoch, height, slot from \"Block\" " +
                    "where height = ?"
            conn.prepareStatement(sql).use { statement ->
                statement.setLong(1,height)
                val rs: ResultSet = statement.executeQuery()
                if (rs.next()) {
                    BlockView(
                        rs.getString("hash"),
                        rs.getInt("epoch"),
                        rs.getLong("height"),
                        rs.getLong("slot"))
                }
                else {
                    return null
                }
            }
        }
        return latestBlock
    }
}