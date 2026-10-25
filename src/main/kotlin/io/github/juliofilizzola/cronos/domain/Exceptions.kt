package io.github.juliofilizzola.cronos.domain

class NotFoundException(message: String) : RuntimeException(message)
class ConflictException(message: String) : RuntimeException(message)
class InvalidScheduleException(message: String) : RuntimeException(message)