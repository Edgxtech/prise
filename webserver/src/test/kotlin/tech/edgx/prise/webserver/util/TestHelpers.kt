package tech.edgx.prise.webserver.util

import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.*
import kotlin.system.exitProcess

object TestHelpers {

    @Suppress("UNCHECKED_CAST")
    fun <T> getProp(key: String): T {
        val props  = javaClass.classLoader.getResourceAsStream("application.properties").use {
            Properties().apply { load(it) }
        }
        return (props.getProperty(key) as T) ?: throw RuntimeException("could not find property $key")
    }

    fun populateDatabaseWithTestData() {
        /* Test data in src/test/resources/db/prise-01Aug24-07Aug24-m2data.sql.gz contains
           678 assets and candles for ~1 week */
        val username: String = getProp("spring.datasource.username")
        val password: String = getProp("spring.datasource.password")
        val dbName = "prise"
        val connectionProps = Properties()
        connectionProps.put("user", username)
        connectionProps.put("password", password)
        val conn: Connection = try {
            Class.forName("com.mysql.cj.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/", connectionProps)
        } catch (ex: Exception) {
            ex.printStackTrace()
            exitProcess(1)
        }
        val stmt: Statement = conn.createStatement()
        stmt.executeUpdate("CREATE DATABASE if not exists $dbName")

        val existsAndPopulated = try {
            val result = stmt.executeQuery("select count(*) as count from $dbName.asset " +
                    "union select count(*) from $dbName.candle_weekly " +
                    "union select count(*) from $dbName.candle_daily " +
                    "union select count(*) from $dbName.candle_hourly " +
                    "union select count(*) from $dbName.candle_fifteen ")
            val listOfCounts = result.use {
                generateSequence {
                    if (result.next()) result.getInt(1) else null
                }.toList()  // must be inside the use() block
            }
            //println("All counts: $listOfCounts")
            (listOfCounts[0]==678 && listOfCounts[1]==362 && listOfCounts[2]==2175 && listOfCounts[3]==47389 && listOfCounts[4]==190223)
        } catch (e: Exception) {
            println("Couldn't determine state of test db, will attempt to regenerate..")
            false
        }
        println("Test data exists and populated: $existsAndPopulated")
        if (!existsAndPopulated) {
            println("Rebuilding test dataset")
            stmt.executeUpdate("DROP DATABASE if exists $dbName;")
            stmt.executeUpdate("CREATE DATABASE if not exists $dbName;")

            val cmd = System.getProperty("user.dir") + System.getProperty("file.separator") + "src/test/resources/db/restore.sh"
            ProcessBuilder(cmd, username, password, "prise-01Aug24-07Aug24-m2data.sql.gz", "prise")
                .directory(File(System.getProperty("user.dir")))
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start()
                .waitFor()
        }
    }
}