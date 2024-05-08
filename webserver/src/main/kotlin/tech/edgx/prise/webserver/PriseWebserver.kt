package tech.edgx.prise.webserver

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CardanoPriceIndexerApplication

fun main(args: Array<String>) {
	runApplication<CardanoPriceIndexerApplication>(*args)
}