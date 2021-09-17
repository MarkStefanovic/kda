package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.CopyTableResult
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.KDAError
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

  val includeFieldsDef = if (includeFields == null) {
    srcTableDef.fields
  } else {
    srcTableDef.fields
      .filter { fld -> fld.name in includeFields }
      .toSet()
  }

  val destTableDef = srcTableDef.copy(
    schema = destSchema,
    name = destTable,
    fields = includeFieldsDef,
    primaryKeyFieldNames = primaryKeyFields,
  )

  val dest: Datasource = when (destDialect) {
    Dialect.HortonworksHive -> hiveDatasource(con = destCon)
    Dialect.PostgreSQL -> pgDatasource(con = destCon)
  }

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
