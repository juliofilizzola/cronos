package io.github.juliofilizzola.cronos.api

import io.github.juliofilizzola.cronos.domain.ConflictException
import io.github.juliofilizzola.cronos.domain.InvalidPayloadException
import io.github.juliofilizzola.cronos.domain.InvalidScheduleException
import io.github.juliofilizzola.cronos.domain.NotFoundException
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class ApiExceptionHandler {

    @ExceptionHandler(NotFoundException::class)
    fun notFound(e: NotFoundException): ProblemDetail =
        ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.message ?: "Não encontrado")

    @ExceptionHandler(ConflictException::class)
    fun conflict(e: ConflictException): ProblemDetail =
        ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.message ?: "Conflito")

    @ExceptionHandler(InvalidScheduleException::class)
    fun invalidSchedule(e: InvalidScheduleException): ProblemDetail =
        ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.message ?: "Agendamento inválido")

    @ExceptionHandler(InvalidPayloadException::class)
    fun invalidPayload(e: InvalidPayloadException): ProblemDetail =
        ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.message ?: "Payload inválido")
}