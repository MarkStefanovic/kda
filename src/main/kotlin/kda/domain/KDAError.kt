package kda.domain

import java.sql.JDBCType

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

  data class PrimaryKeyFieldNotFound(
    val fieldName: String,
    val availableFields: List<String>,
  ) : KDAError() {
    override val errorMessage: String =
      "Primary key field, $fieldName, was not found.  Available fields include the following: " +
        "${availableFields.joinToString(", ")}."
  }

  data class TableMissingAPrimaryKey(val schema: String?, val table: String) : KDAError() {
    override val errorMessage: String by lazy {
      val fullTableName = if (schema == null) {
        table
      } else {
        "$schema.$table"
      }
      "The table, $fullTableName, does not have a primary key."
    }
  }

  data class UnrecognizeDataType(val dataTypeName: String) : KDAError() {
    override val errorMessage: String =
      "The dataType, $dataTypeName, is not recognized.  Recognized dataTypes include bigint, " +
        "decimal, int, date, datetime, text."

    constructor(dataType: DataType<*>) : this(dataTypeName = dataType.description)

    constructor(jdbcType: JDBCType) : this(dataTypeName = jdbcType.name)
  }
}
