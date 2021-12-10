package kda.domain

import java.sql.JDBCType

sealed class KDAError(errorMessage: String) : Exception(errorMessage) {
  data class FieldNotFound(
    val fieldName: String,
    val availableFieldNames: Set<String>,
  ) : KDAError(
    "A field named $fieldName was not found.  " +
      "Available fields include the following: ${availableFieldNames.joinToString(", ")}"
  )

  data class NoPrimaryKeySpecified(val table: String) :
    KDAError(
      "$table does not have a primary key at the database level, and no primary keys were specified."
    )

  data class InvalidArgument(
    val errorMessage: String,
    val argumentName: String,
    val argumentValue: Any,
  ) : KDAError(errorMessage)

  data class TableNotFound(
    val schema: String?,
    val table: String,
  ) : KDAError(
    "A table named ${fullTableName(schema = schema, table = table)} was not found in the database."
  )

  data class NoRowsReturned(val sql: String) :
    KDAError("The following query returned no results: $sql")

  data class PrimaryKeyFieldNotFound(
    val fieldName: String,
    val availableFields: List<String>,
  ) : KDAError(
    "Primary key field, $fieldName, was not found.  Available fields include the following: " +
      "${availableFields.joinToString(", ")}."
  )

  data class TableMissingAPrimaryKey(val schema: String?, val table: String) :
    KDAError(
      "The table, ${fullTableName(schema = schema, table = table)}, does not have a primary key."
    )

  data class UnrecognizeDataType(val dataTypeName: String) : KDAError(
    "The dataType, $dataTypeName, is not recognized.  Recognized dataTypes include bigint, " +
      "decimal, int, date, datetime, text."
  ) {
    constructor(dataType: DataType<*>) : this(dataTypeName = dataType.description)

    constructor(jdbcType: JDBCType) : this(dataTypeName = jdbcType.name)
  }
}

private fun fullTableName(schema: String?, table: String): String =
  if (schema == null) {
    table
  } else {
    "$schema.$table"
  }
