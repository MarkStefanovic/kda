package service

import adapter.pg.pgDatasource
import domain.*
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
): CopyTableResult {
  try {
    val src: Datasource =
      when (srcDialect) {
        Dialect.HortonworksHive -> TODO()
        Dialect.PostgreSQL -> pgDatasource(con = srcCon)
      }

    val dest: Datasource =
      when (destDialect) {
        Dialect.HortonworksHive -> TODO()
        Dialect.PostgreSQL -> pgDatasource(con = destCon)
      }

    val srcTableDef =
      src.inspector.inspectTable(
        schema = srcSchema,
        table = srcTable,
        maxFloatDigits = 5,
      )

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
        val createTableSQL = dest.adapter.createTable(table = destTableDef)
        dest.executor.execute(sql = createTableSQL)
        true
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
    return CopyTableResult.Error(
      srcDialect = srcDialect,
      destDialect = destDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      includeFields = includeFields,
      primaryKeyFields = primaryKeyFields,
      error = CopyTableError(errorMessage = e.message ?: "No error message was provided.", originalError = e),
    )
  }
}
