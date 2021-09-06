package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.CopyTableResult
import kda.domain.Datasource
import kda.domain.Dialect
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
  includeFields: Set<String>,
  primaryKeyFields: List<String>,
  cache: Cache = DbCache(),
): CopyTableResult {
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

    val srcTableDef =
      try {
        val cachedTableDef = cache.tableDef(
          schema = srcSchema ?: "",
          table = srcTable
        )
        if (cachedTableDef == null) {
          val tableDef = src.inspector.inspectTable(
            schema = srcSchema,
            table = srcTable,
            maxFloatDigits = 5,
            primaryKeyFieldNames = primaryKeyFields,
          )
          cache.addTableDef(tableDef)
          tableDef
        } else {
          cachedTableDef
        }
      } catch (e: Exception) {
        return CopyTableResult.Error.InspectTableFailed(
          srcDialect = srcDialect,
          destDialect = destDialect,
          srcSchema = srcSchema,
          srcTable = srcTable,
          destSchema = destSchema,
          destTable = destTable,
          includeFields = includeFields,
          primaryKeyFields = primaryKeyFields,
          errorMessage = "An unexpected error occurred while executing inspectTable on src: ${e.message}",
          originalError = e,
        )
      }

    val includeFieldsDef = srcTableDef.fields.filter { fld -> fld.name in includeFields }.toSet()

    val destTableDef =
      srcTableDef.copy(
        schema = destSchema,
        name = destTable,
        fields = includeFieldsDef,
        primaryKeyFieldNames = primaryKeyFields,
      )

    val created =
      if (dest.inspector.tableExists(schema = destSchema, table = destTable)) {
        false
      } else {
        try {
          val createTableSQL = dest.adapter.createTable(table = destTableDef)
          dest.executor.execute(sql = createTableSQL)
          true
        } catch (e: Exception) {
          return CopyTableResult.Error.CreateTableFailed(
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

    return CopyTableResult.Success(
      srcDialect = srcDialect,
      destDialect = destDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      includeFields = includeFields,
      primaryKeyFields = primaryKeyFields,
      srcTableDef = srcTableDef,
      destTableDef = destTableDef,
      created = created,
    )
  } catch (e: Exception) {
    return CopyTableResult.Error.Unexpected(
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
}
