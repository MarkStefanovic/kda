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
  includeFieldNames: Set<String>? = null,
  criteria: Set<Criteria> = emptySet(),
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
      includeFieldNames = includeFieldNames,
      cache = cache,
    )

    when (srcTableDefResult) {
      is InspectTableResult.Error -> CompareRowsResult.Error.InspectTableFailed(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = srcTableDefResult.errorMessage,
        originalError = srcTableDefResult.originalError,
      )
      is InspectTableResult.Success -> {
        val destTableDefResult = inspectTable(
          con = destCon,
          dialect = destDialect,
          schema = destSchema,
          table = destTable,
          primaryKeyFieldNames = primaryKeyFieldNames,
          includeFieldNames = includeFieldNames,
          cache = cache,
        )
        when (destTableDefResult) {
          is InspectTableResult.Error -> CompareRowsResult.Error.InspectTableFailed(
            srcSchema = srcSchema,
            srcTable = srcTable,
            destSchema = destSchema,
            destTable = destTable,
            errorMessage = destTableDefResult.errorMessage,
            originalError = destTableDefResult.originalError,
          )
          is InspectTableResult.Success -> {
            val srcRowsSQL: String = src.adapter.select(
              table = srcTableDefResult.tableDef,
              criteria = criteria,
            )
            val srcRows: Int = src.executor.fetchInt(srcRowsSQL)

            val destRowsSQL: String = dest.adapter.select(
              table = destTableDefResult.tableDef,
              criteria = criteria,
            )
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
