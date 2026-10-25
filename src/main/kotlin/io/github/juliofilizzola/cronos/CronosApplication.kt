package io.github.juliofilizzola.cronos

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CronosApplication

fun main(args: Array<String>) {
    runApplication<CronosApplication>(*args)
}
