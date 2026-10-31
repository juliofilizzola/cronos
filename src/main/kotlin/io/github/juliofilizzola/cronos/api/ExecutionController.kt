package io.github.juliofilizzola.cronos.api

import io.github.juliofilizzola.cronos.domain.ExecutionView
import io.github.juliofilizzola.cronos.service.ExecutionService
import org.springframework.web.bind.annotation.*
import java.util.UUID

@RestController
@RequestMapping("/executions")
class ExecutionController(private val service: ExecutionService) {

    @GetMapping("/{id}")
    fun get(@PathVariable("id") id: UUID): ExecutionView = service.get(id)

    @PostMapping("/{id}/cancel")
    fun cancel(@PathVariable("id") id: UUID): ExecutionView = service.cancel(id)

    @PostMapping("/{id}/retry")
    fun retry(@PathVariable("id") id: UUID): ExecutionView = service.retry(id)
}
