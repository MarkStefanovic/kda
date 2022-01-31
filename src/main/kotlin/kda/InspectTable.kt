package kda

import kda.domain.Cache
import kda.domain.KDAError
import kda.domain.Table
import java.sql.Connection

fun inspectTable(
  con: Connection,
  cache: Cache,
  dbName: String,
  schema: String?,
  table: String,
  primaryKeyFieldNames: List<String>,
  includeFieldNames: Set<String>?,
): Table {
  if ((includeFieldNames != null) && (includeFieldNames.isEmpty())) {
    throw KDAError.InvalidArgument(
      errorMessage = "If includeFieldNames is not null, then it must have at least 1 field.",
      argumentName = "includeFieldNames",
      argumentValue = includeFieldNames,
    )
  } else if (primaryKeyFieldNames.isEmpty()) {
    throw KDAError.InvalidArgument(
      errorMessage = "primaryKeyFieldNames cannot be blank.",
      argumentName = "primaryKeyFieldNames",
      argumentValue = primaryKeyFieldNames,
    )
  } else {
    val cachedTable = cache.getTable(dbName = dbName, schema = schema, table = table)

    val tableDef =
      if (cachedTable == null) {
        val def = kda.adapter.inspectTable(
          con = con,
          schema = schema,
          table = table,
          hardCodedPrimaryKeyFieldNames = primaryKeyFieldNames,
        )
        cache.addTable(dbName = dbName, schema = schema, table = def)
        def
      } else {
        cachedTable
      }

    val missingPrimaryKeyFields: Set<String> =
      primaryKeyFieldNames
        .toSet()
        .minus(tableDef.fields.map { it.name }.toSet())

    return if (missingPrimaryKeyFields.isEmpty()) {
      val missingIncludeFields: Set<String> =
        includeFieldNames
          ?.minus(tableDef.fields.map { it.name }.toSet())
          ?: setOf()

      if (missingIncludeFields.isNotEmpty()) {
        val includeFieldsCSV = includeFieldNames?.joinToString(", ") ?: ""

        val missingIncludeFieldsCSV = missingPrimaryKeyFields.joinToString(", ")

        val errorMessage =
          "The includeFields specified, [$includeFieldsCSV], does not include the " +
            "following primary-key fields: $missingIncludeFieldsCSV."

        throw KDAError.InvalidArgument(
          errorMessage = errorMessage,
          argumentName = "includeFieldNames",
          argumentValue = "[$includeFieldsCSV]",
        )
      } else {
        if (includeFieldNames == null) {
          tableDef
        } else {
          val finalFields =
            tableDef
              .fields
              .filter { fld -> fld.name in includeFieldNames }
              .toSet()

          tableDef.copy(fields = finalFields)
        }
      }
    } else {
      val pkFieldNamesCSV = primaryKeyFieldNames.joinToString(", ")

      val missingFieldsCSV = missingPrimaryKeyFields.joinToString(", ")

      val fieldNameCSV = tableDef.fields.map { it.name }.sorted().joinToString(", ")

      val errorMessage =
        "The following primary key field(s) were specified: $pkFieldNamesCSV.  " +
          "However, the table does not include the following fields: $missingFieldsCSV.  " +
          "The table includes the following fields: $fieldNameCSV"

      throw KDAError.InvalidArgument(
        errorMessage = errorMessage,
        argumentName = "primaryKeyFieldNames",
        argumentValue = primaryKeyFieldNames,
      )
    }
  }
}
