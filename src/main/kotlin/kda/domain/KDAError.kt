package kda.domain

sealed class KDAError : Exception() {
  abstract val errorMessage: String

  data class SQLError(
    val sql: String,
    val originalError: Exception,
  ) : KDAError() {
    override val errorMessage by lazy {
      "The following error occurred while executing '$sql': ${originalError.message}"
    }
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
      val fullTableName = if (schema == null) {
        table
      } else {
        "$schema.$table"
      }
      "A table named $fullTableName was not found in the database."
    }
  }
}

private fun fullTableName(schema: String?, table: String) =
  if (schema == null) {
    table
  } else {
    "$schema.$table"
  }
