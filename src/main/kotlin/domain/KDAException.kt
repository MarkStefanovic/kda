package domain

abstract class KDAException(
  message: String,
) : Exception(message)

data class NoRowsReturned(val sql: String) :
  KDAException("The following query returned no results: $sql")

data class NullValueError(val expectedType: String) :
  KDAException("Expected a $expectedType value, but the value was null.")

data class ValueError(
  val value: Any?,
  val expectedType: String,
) :
  KDAException(
    "Expected a $expectedType value, but got '$value' of type ${value?.javaClass?.simpleName ?: "null"}."
  )
