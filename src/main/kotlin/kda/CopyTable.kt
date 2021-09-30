package kda

import kda.domain.CopyTableResult
import kda.domain.Dialect
import kda.domain.IntType
import kda.domain.KDAError
import kda.domain.NullableIntType
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
  cache: Cache = sqliteCache,
  ignoreAutoincrement: Boolean = true,
): Result<CopyTableResult> = runCatching {
  val srcTableDef = inspectTable(
    con = srcCon,
    dialect = srcDialect,
    schema = srcSchema,
    table = srcTable,
    primaryKeyFieldNames = primaryKeyFields,
    includeFieldNames = includeFields,
    cache = cache,
  ).getOrThrow() ?: throw KDAError.TableNotFound(schema = srcSchema, table = srcTable)

  val includeFieldDefs = if (includeFields == null) {
    srcTableDef.fields
  } else {
    srcTableDef.fields
      .filter { fld -> fld.name in includeFields }
      .toSet()
  }

  val fields = if (ignoreAutoincrement) {
    includeFieldDefs.map { fld ->
      when (fld.dataType) {
        is IntType, is NullableIntType -> fld.copy(dataType = IntType(false))
        else -> fld
      }
    }.toSet()
  } else {
    includeFieldDefs
  }

  val destTableDef = srcTableDef.copy(
    schema = destSchema,
    name = destTable,
    fields = fields,
    primaryKeyFieldNames = primaryKeyFields,
  )

  val dest = datasource(con = destCon, dialect = destDialect)

  val create = !dest.inspector.tableExists(schema = destSchema, table = destTable)
  if (create) {
    val createTableSQL = dest.adapter.createTable(table = destTableDef)
    dest.executor.execute(sql = createTableSQL)
  }

  CopyTableResult(
    srcTableDef = srcTableDef,
    destTableDef = destTableDef,
  )
}
