package io.github.juliofilizzola.cronos

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
    fromApplication<CronosApplication>().with(TestcontainersConfiguration::class).run(*args)
}
