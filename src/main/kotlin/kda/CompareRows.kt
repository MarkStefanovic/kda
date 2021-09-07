package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.CompareRowsResult
import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.InspectTableResult
import java.sql.Connection

fun compareRows(
  srcCon: Connection,
  destCon: Connection,
  srcDialect: Dialect,
  destDialect: Dialect,
  srcSchema: String?,
  srcTable: String,
  destSchema: String?,
  destTable: String,
  primaryKeyFieldNames: List<String>,
  criteria: List<Criteria> = emptyList(),
  cache: Cache = DbCache(),
): CompareRowsResult =
  try {
    val src: Datasource =
      when (srcDialect) {
        Dialect.HortonworksHive -> hiveDatasource(con = srcCon)
        Dialect.PostgreSQL -> pgDatasource(con = srcCon)
      }

    val dest: Datasource =
      when (destDialect) {
        Dialect.HortonworksHive -> hiveDatasource(con = destCon)
        Dialect.PostgreSQL -> pgDatasource(con = destCon)
      }

    val srcTableDefResult = inspectTable(
      con = srcCon,
      dialect = srcDialect,
      schema = srcSchema,
      table = srcTable,
      primaryKeyFieldNames = primaryKeyFieldNames,
      cache = cache,
    )
    when (val result = srcTableDefResult) {
      is InspectTableResult.Error.InspectTableFailed -> CompareRowsResult.Error.InspectTableFailed(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = result.errorMessage,
        originalError = result.originalError,
      )
      is InspectTableResult.Error.InvalidArgument -> CompareRowsResult.Error.InvalidArgument(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        argumentName = result.argumentName,
        argumentValue = result.argumentValue,
        errorMessage = result.errorMessage,
        originalError = result.originalError,
      )
      is InspectTableResult.Error.Unexpected -> CompareRowsResult.Error.Unexpected(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = result.errorMessage,
        originalError = result.originalError,
      )
      is InspectTableResult.Success -> {
        val destTableDef = result.tableDef.copy(schema = destSchema, name = destTable)

        val srcRowsSQL: String = src.adapter.select(table = result.tableDef, criteria = criteria)
        val srcRows: Int = src.executor.fetchInt(srcRowsSQL)

        val destRowsSQL: String = dest.adapter.select(table = destTableDef, criteria = criteria)
        val destRows = dest.executor.fetchInt(destRowsSQL)

        CompareRowsResult.Success(
          srcSchema = srcSchema,
          srcTable = srcTable,
          destSchema = destSchema,
          destTable = destTable,
          srcRows = srcRows,
          destRows = destRows,
        )
      }
    }
  } catch (e: Exception) {
    CompareRowsResult.Error.Unexpected(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      errorMessage = "An unexpected error occurred while executing inspectTable: ${e.message}",
      originalError = e,
    )
  }
