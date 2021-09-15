package kda.domain

sealed class KDAError(val errorMessage: String) : Exception() {
  data class SQLError(
    val sql: String,
    val originalError: Exception,
  ) : KDAError("The following error occurred while executing '$sql': ${originalError.message}")
}

private fun fullTableName(schema: String?, table: String) =
  if (schema == null) {
    table
  } else {
    "$schema.$table"
  }
