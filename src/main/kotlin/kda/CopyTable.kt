package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.CopyTableResult
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.InspectTableResult
import java.sql.Connection

fun copyTable(
  srcCon: Connection,
  destCon: Connection,
  srcDialect: Dialect,
  destDialect: Dialect,
  srcSchema: String?,
  srcTable: String,
  destSchema: String?,
  destTable: String,
  primaryKeyFields: List<String>,
  includeFields: Set<String>? = null,
  cache: Cache = DbCache(),
): CopyTableResult =
  try {
    val src: Datasource =
      when (srcDialect) {
        Dialect.HortonworksHive -> hiveDatasource(con = srcCon)
        Dialect.PostgreSQL -> pgDatasource(con = srcCon)
      }

    if (!src.inspector.tableExists(schema = srcSchema, table = srcTable)) {
      CopyTableResult.Error.SourceTableDoesNotExist(
        srcDialect = srcDialect,
        destDialect = destDialect,
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        includeFields = includeFields,
        primaryKeyFields = primaryKeyFields,
      )
    } else {
      val inspectSrcResult = inspectTable(
        con = srcCon,
        dialect = srcDialect,
        schema = srcSchema,
        table = srcTable,
        primaryKeyFieldNames = primaryKeyFields,
        includeFieldNames = includeFields,
        cache = cache,
      )
      when (inspectSrcResult) {
        is InspectTableResult.Error.TableDoesNotExist -> CopyTableResult.Error.SourceTableDoesNotExist(
          srcDialect = srcDialect,
          destDialect = destDialect,
          srcSchema = srcSchema,
          srcTable = srcTable,
          destSchema = destSchema,
          destTable = destTable,
          includeFields = includeFields,
          primaryKeyFields = primaryKeyFields,
        )
        is InspectTableResult.Error.InspectTableFailed -> CopyTableResult.Error.InspectTableFailed(
          srcDialect = srcDialect,
          destDialect = destDialect,
          srcSchema = srcSchema,
          srcTable = srcTable,
          destSchema = destSchema,
          destTable = destTable,
          includeFields = includeFields,
          primaryKeyFields = primaryKeyFields,
          errorMessage = inspectSrcResult.errorMessage,
          originalError = inspectSrcResult.originalError,
        )
        is InspectTableResult.Error.InvalidArgument -> CopyTableResult.Error.InvalidArgument(
          srcDialect = srcDialect,
          destDialect = destDialect,
          srcSchema = srcSchema,
          srcTable = srcTable,
          destSchema = destSchema,
          destTable = destTable,
          includeFields = includeFields,
          primaryKeyFields = primaryKeyFields,
          errorMessage = inspectSrcResult.errorMessage,
          originalError = inspectSrcResult.originalError,
          argumentName = inspectSrcResult.argumentName,
          argumentValue = inspectSrcResult.argumentValue,
        )
        is InspectTableResult.Error.Unexpected -> CopyTableResult.Error.Unexpected(
          srcDialect = srcDialect,
          destDialect = destDialect,
          srcSchema = srcSchema,
          srcTable = srcTable,
          destSchema = destSchema,
          destTable = destTable,
          includeFields = includeFields,
          primaryKeyFields = primaryKeyFields,
          errorMessage = inspectSrcResult.errorMessage,
          originalError = inspectSrcResult.originalError,
        )
        is InspectTableResult.Success -> {
          val includeFieldsDef = if (includeFields == null) {
            inspectSrcResult.tableDef.fields
          } else {
            inspectSrcResult.tableDef.fields
              .filter { fld -> fld.name in includeFields }
              .toSet()
          }

          val destTableDef =
            inspectSrcResult.tableDef.copy(
              schema = destSchema,
              name = destTable,
              fields = includeFieldsDef,
              primaryKeyFieldNames = primaryKeyFields,
            )

          val dest: Datasource =
            when (destDialect) {
              Dialect.HortonworksHive -> hiveDatasource(con = destCon)
              Dialect.PostgreSQL -> pgDatasource(con = destCon)
            }

          val create = !dest.inspector.tableExists(schema = destSchema, table = destTable)
          try {
            if (create) {
              val createTableSQL = dest.adapter.createTable(table = destTableDef)
              dest.executor.execute(sql = createTableSQL)
            }
            CopyTableResult.Success(
              srcDialect = srcDialect,
              destDialect = destDialect,
              srcSchema = srcSchema,
              srcTable = srcTable,
              destSchema = destSchema,
              destTable = destTable,
              includeFields = includeFields,
              primaryKeyFields = primaryKeyFields,
              srcTableDef = inspectSrcResult.tableDef,
              destTableDef = destTableDef,
              created = create,
            )
          } catch (e: Exception) {
            CopyTableResult.Error.CreateTableFailed(
              srcDialect = srcDialect,
              destDialect = destDialect,
              srcSchema = srcSchema,
              srcTable = srcTable,
              destSchema = destSchema,
              destTable = destTable,
              includeFields = includeFields,
              primaryKeyFields = primaryKeyFields,
              errorMessage = "An unexpected error occurred while creating the dest table: ${e.message}",
              originalError = e,
            )
          }
        }
      }
    }
  } catch (e: Exception) {
    CopyTableResult.Error.Unexpected(
      srcDialect = srcDialect,
      destDialect = destDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      includeFields = includeFields,
      primaryKeyFields = primaryKeyFields,
      errorMessage = "An unexpected error occurred while executing copyTable: ${e.message}",
      originalError = e,
    )
  }
