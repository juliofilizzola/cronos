package io.github.juliofilizzola.cronos.api

import io.github.juliofilizzola.cronos.domain.ExecutionView
import io.github.juliofilizzola.cronos.domain.JobDetails
import io.github.juliofilizzola.cronos.domain.JobStatus
import io.github.juliofilizzola.cronos.service.ExecutionService
import io.github.juliofilizzola.cronos.service.JobService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import java.util.UUID

@RestController
@RequestMapping("/jobs")
class JobController(
    private val service: JobService,
    private val executions: ExecutionService,
) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun create(@Valid @RequestBody request: CreateJobRequest): JobDetails = service.create(request)

    @GetMapping
    fun list(
        @RequestParam("status", required = false) status: JobStatus?,
        @RequestParam("page", defaultValue = "0") page: Int,
        @RequestParam("size", defaultValue = "20") size: Int,
    ): List<JobDetails> = service.list(status, page.coerceAtLeast(0), size.coerceIn(1, 100))

    @GetMapping("/{id}")
    fun get(@PathVariable("id") id: UUID): JobDetails = service.get(id)

    @GetMapping("/{id}/executions")
    fun executions(
        @PathVariable("id") id: UUID,
        @RequestParam("page", defaultValue = "0") page: Int,
        @RequestParam("size", defaultValue = "20") size: Int,
    ): List<ExecutionView> = service.executions(id, page.coerceAtLeast(0), size.coerceIn(1, 100))

    /** Dispara o job agora, fora do agendamento. */
    @PostMapping("/{id}/trigger")
    @ResponseStatus(HttpStatus.CREATED)
    fun trigger(@PathVariable("id") id: UUID): ExecutionView = executions.trigger(id)

    @PostMapping("/{id}/pause")
    fun pause(@PathVariable("id") id: UUID): JobDetails = service.pause(id)

    @PostMapping("/{id}/resume")
    fun resume(@PathVariable("id") id: UUID): JobDetails = service.resume(id)
}