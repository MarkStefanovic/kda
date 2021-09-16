package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.mssql.mssqlDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.CacheResult
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
  includeFieldNames: Set<String>?,
  cache: Cache = DbCache(),
): InspectTableResult =
  try {
    if ((includeFieldNames != null) && (includeFieldNames.isEmpty())) {
      InspectTableResult.Error.InvalidArgument(
        schema = schema,
        table = table,
        dialect = dialect,
        primaryKeyFieldNames = primaryKeyFieldNames,
        errorMessage = "If includeFieldNames is not null, then it must have at least 1 field.",
        originalError = null,
        argumentName = "includeFieldNames",
        argumentValue = includeFieldNames,
      )
    } else if (primaryKeyFieldNames.isEmpty()) {
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
      val ds: Datasource =
        when (dialect) {
          Dialect.HortonworksHive -> hiveDatasource(con = con)
          Dialect.MSSQLServer -> mssqlDatasource(con = con)
          Dialect.PostgreSQL -> pgDatasource(con = con)
        }
      try {
        val cachedTableDefResult = cache.tableDef(
          schema = schema ?: "",
          table = table
        )
        when (cachedTableDefResult) {
          is CacheResult.TableDef.Error -> InspectTableResult.Error.CacheError(
            schema = schema,
            table = table,
            dialect = dialect,
            primaryKeyFieldNames = primaryKeyFieldNames,
            errorMessage = cachedTableDefResult.errorMessage,
            originalError = cachedTableDefResult.originalError,
          )
          is CacheResult.TableDef.Success -> {
            val tableDef = if (cachedTableDefResult.tableDef == null) {
              val def = ds.inspector.inspectTable(
                schema = schema,
                table = table,
                maxFloatDigits = 5,
                primaryKeyFieldNames = primaryKeyFieldNames,
              )
              cache.addTableDef(def)
              def
            } else {
              cachedTableDefResult.tableDef
            }
            val missingPrimaryKeyFields: Set<String> =
              primaryKeyFieldNames.toSet().minus(tableDef.sortedFieldNames.toSet())
            if (missingPrimaryKeyFields.isEmpty()) {
              val missingIncludeFields: Set<String> = includeFieldNames?.minus(tableDef.sortedFieldNames.toSet()) ?: setOf()
              if (missingIncludeFields.isNotEmpty()) {
                val includeFieldsCSV = includeFieldNames?.joinToString(", ") ?: ""
                val missingIncludeFieldsCSV = missingPrimaryKeyFields.joinToString(", ")
                val errorMessage =
                  "The includeFields specified, [$includeFieldsCSV], does not include the " +
                    "following primary-key fields: $missingIncludeFieldsCSV."
                InspectTableResult.Error.InvalidArgument(
                  schema = schema,
                  table = table,
                  dialect = dialect,
                  primaryKeyFieldNames = primaryKeyFieldNames,
                  errorMessage = errorMessage,
                  originalError = null,
                  argumentName = "includeFieldNames",
                  argumentValue = includeFieldNames,
                )
              } else {
                val finalTableDef = if (includeFieldNames == null) {
                  tableDef
                } else {
                  val finalFields = tableDef.fields.filter { fld -> fld.name in includeFieldNames }.toSet()
                  tableDef.copy(fields = finalFields)
                }
                InspectTableResult.Success(
                  schema = schema,
                  table = table,
                  dialect = dialect,
                  primaryKeyFieldNames = primaryKeyFieldNames,
                  tableDef = finalTableDef,
                )
              }
            } else {
              val pkFieldNamesCSV = primaryKeyFieldNames.joinToString(", ")
              val missingFieldsCSV = missingPrimaryKeyFields.joinToString(", ")
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
