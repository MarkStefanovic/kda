package kda

import kda.domain.CopyTableResult
import kda.domain.DataType
import kda.domain.Dialect
import kda.domain.KDAError
import java.sql.Connection

fun copyTable(
  srcCon: Connection,
  destCon: Connection,
  cacheCon: Connection,
  srcDialect: Dialect,
  destDialect: Dialect,
  cacheDialect: Dialect,
  srcSchema: String?,
  srcTable: String,
  destSchema: String?,
  destTable: String,
  primaryKeyFields: List<String>,
  includeFields: Set<String>? = null,
  ignoreAutoincrement: Boolean = true,
): Result<CopyTableResult> = runCatching {

  val srcTableDef = inspectTable(
    con = srcCon,
    cacheCon = cacheCon,
    dialect = srcDialect,
    cacheDialect = cacheDialect,
    schema = srcSchema,
    table = srcTable,
    primaryKeyFieldNames = primaryKeyFields,
    includeFieldNames = includeFields,
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
        is DataType.int -> fld.copy(dataType = DataType.int(false))
        is DataType.nullableInt -> fld.copy(dataType = DataType.nullableInt(false))
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
