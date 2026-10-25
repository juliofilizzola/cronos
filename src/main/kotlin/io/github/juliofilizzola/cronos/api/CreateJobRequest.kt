package io.github.juliofilizzola.cronos.api

import io.github.juliofilizzola.cronos.domain.MisfirePolicy
import io.github.juliofilizzola.cronos.domain.ScheduleType
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import jakarta.validation.constraints.Size
import tools.jackson.databind.JsonNode

data class CreateJobRequest(
    @field:NotBlank @field:Size(max = 200) val name: String,
    @field:NotNull val scheduleType: ScheduleType,
    /** CRON: 6 campos com segundos. INTERVAL: ISO-8601 (PT5M). ONCE: instante ISO-8601. */
    @field:NotBlank val scheduleExpr: String,
    val timezone: String = "UTC",
    @field:NotNull val payload: JsonNode,
    val misfirePolicy: MisfirePolicy = MisfirePolicy.FIRE_ONCE,
    @field:Min(0) val misfireThresholdSeconds: Int = 60,
    val allowOverlap: Boolean = false,
    @field:Min(1) @field:Max(20) val maxAttempts: Int = 3,
    @field:Min(1) val timeoutSeconds: Int = 60,
)