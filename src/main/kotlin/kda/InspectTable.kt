package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.InspectTableResult
import java.sql.Connection

fun inspectTable(
  con: Connection,
  dialect: Dialect,
  schema: String?,
  table: String,
  primaryKeyFieldNames: List<String>,
  cache: Cache = DbCache(),
): InspectTableResult =
  try {
    val ds: Datasource =
      when (dialect) {
        Dialect.HortonworksHive -> hiveDatasource(con = con)
        Dialect.PostgreSQL -> pgDatasource(con = con)
      }
    try {
      val cachedTableDef = cache.tableDef(
        schema = schema ?: "",
        table = table
      )
      val tableDef = if (cachedTableDef == null) {
        val def = ds.inspector.inspectTable(
          schema = schema,
          table = table,
          maxFloatDigits = 5,
          primaryKeyFieldNames = primaryKeyFieldNames,
        )
        cache.addTableDef(def)
        def
      } else {
        cachedTableDef
      }
      if (primaryKeyFieldNames.isEmpty()) {
        InspectTableResult.Error.InvalidArgument(
          schema = schema,
          table = table,
          dialect = dialect,
          primaryKeyFieldNames = primaryKeyFieldNames,
          errorMessage = "primaryKeyFieldNames cannot be blank.",
          originalError = null,
          argumentName = "primaryKeyFieldNames",
          argumentValue = primaryKeyFieldNames,
        )
      } else {
        val missingFields: Set<String> =
          primaryKeyFieldNames.toSet().minus(tableDef.sortedFieldNames.toSet())
        if (missingFields.isEmpty()) {
          InspectTableResult.Success(
            schema = schema,
            table = table,
            dialect = dialect,
            primaryKeyFieldNames = primaryKeyFieldNames,
            tableDef = tableDef,
          )
        } else {
          val pkFieldNamesCSV = primaryKeyFieldNames.joinToString(", ")
          val missingFieldsCSV = missingFields.joinToString(", ")
          val fieldNameCSV = tableDef.sortedFieldNames.joinToString(", ")
          val errorMessage =
            "The following primary key field(s) were specified: $pkFieldNamesCSV.  " +
              "However, the table does not include the following fields: $missingFieldsCSV.  " +
              "The table includes the following fields: $fieldNameCSV"
          InspectTableResult.Error.InvalidArgument(
            schema = schema,
            table = table,
            dialect = dialect,
            primaryKeyFieldNames = primaryKeyFieldNames,
            errorMessage = errorMessage,
            originalError = null,
            argumentName = "primaryKeyFieldNames",
            argumentValue = primaryKeyFieldNames,
          )
        }
      }
    } catch (e: Exception) {
      InspectTableResult.Error.InspectTableFailed(
        dialect = dialect,
        schema = schema,
        table = table,
        primaryKeyFieldNames = primaryKeyFieldNames,
        errorMessage = "An unexpected error occurred while executing inspectTable on src: ${e.message}",
        originalError = e,
      )
    }
  } catch (e: Exception) {
    InspectTableResult.Error.Unexpected(
      dialect = dialect,
      schema = schema,
      table = table,
      primaryKeyFieldNames = primaryKeyFieldNames,
      errorMessage = "An unexpected error occurred while executing inspectTable: ${e.message}",
      originalError = e,
    )
  }
