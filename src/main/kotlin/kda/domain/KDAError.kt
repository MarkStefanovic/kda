package kda.domain

sealed class KDAError : Exception() {
  abstract val errorMessage: String

  data class FieldNotFound(
    val fieldName: String,
    val availableFieldNames: Set<String>,
  ) : KDAError() {
    override val errorMessage =
      "A field named $fieldName was not found.  " +
        "Available fields include the following: ${availableFieldNames.joinToString(", ")}"
  }

  data class NoPrimaryKeySpecified(val table: String) : KDAError() {
    override val errorMessage =
      "$table does not have a primary key at the database level, and no primary keys were specified."
  }

  data class SQLError(
    val sql: String,
    val originalError: Exception,
  ) : KDAError() {
    override val errorMessage =
      "The following error occurred while executing '$sql': ${originalError.message}"
  }

  data class InvalidArgument(
    override val errorMessage: String,
    val argumentName: String,
    val argumentValue: Any,
  ) : KDAError()

  data class TableNotFound(
    val schema: String?,
    val table: String,
  ) : KDAError() {
    override val errorMessage by lazy {
      val fullTableName =
        if (schema == null) {
          table
        } else {
          "$schema.$table"
        }
      "A table named $fullTableName was not found in the database."
    }
  }

  data class NoRowsReturned(val sql: String) : KDAError() {
    override val errorMessage = "The following query returned no results: $sql"
  }

  data class NullValueError(val expectedType: String) : KDAError() {
    override val errorMessage = "Expected a $expectedType value, but the value was null."
  }

  data class ValueDataTypeMismatch(
    val value: Value<*>,
    val dataType: DataType<*>,
  ) : KDAError() {
    override val errorMessage: String =
      "value is of type ${value::class.simpleName}, but dataType is of type ${dataType::class.simpleName}."
  }

  data class ValueError(val value: Any?, val expectedType: String) : KDAError() {
    override val errorMessage =
      "Expected a $expectedType value, but got '$value' of type ${value?.javaClass?.simpleName ?: "null"}."
  }
}

private fun fullTableName(schema: String?, table: String) =
  if (schema == null) {
    table
  } else {
    "$schema.$table"
  }
