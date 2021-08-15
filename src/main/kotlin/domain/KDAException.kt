package domain

abstract class KDAException(
  message: String,
  open val originalError: Exception?,
) : Exception(message)

data class CopyTableError(
  val errorMessage: String,
  override val originalError: Exception? = null,
) :
  KDAException(
    message = errorMessage,
    originalError = originalError,
  )

data class NoRowsReturned(val sql: String) :
  KDAException(
    message = "The following query returned no results: $sql",
    originalError = null,
  )

data class NullValueError(val expectedType: String) :
  KDAException(
    message = "Expected a $expectedType value, but the value was null.",
    originalError = null,
  )

data class ValueError(
  val value: Any?,
  val expectedType: String,
) :
  KDAException(
    message =
    "Expected a $expectedType value, but got '$value' of type ${value?.javaClass?.simpleName ?: "null"}.",
    originalError = null,
  )
