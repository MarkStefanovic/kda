package kda

import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.KDAError
import kda.domain.Row
import kda.domain.RowDiff
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
  compareFields: Set<String>,
  primaryKeyFieldNames: List<String>,
  criteria: Set<Criteria> = emptySet(),
  cache: Cache = sqliteCache,
): Result<RowDiff> = runCatching {
  val src = datasource(con = srcCon, dialect = srcDialect)

  val dest = datasource(con = destCon, dialect = destDialect)

  val includeFieldNames = primaryKeyFieldNames.toSet().union(compareFields)

  val srcRows = fetchLookupTable(
    ds = src,
    con = srcCon,
    dialect = srcDialect,
    schema = srcSchema,
    table = srcTable,
    primaryKeyFieldNames = primaryKeyFieldNames,
    compareFields = compareFields,
    criteria = criteria,
    cache = cache,
  ).getOrThrow()

  val destRows = fetchLookupTable(
    ds = dest,
    con = destCon,
    dialect = destDialect,
    schema = destSchema,
    table = destTable,
    primaryKeyFieldNames = primaryKeyFieldNames,
    compareFields = compareFields,
    criteria = criteria,
    cache = cache,
  ).getOrThrow()

  kda.domain.compareRows(
    dest = destRows,
    src = srcRows,
    primaryKeyFields = primaryKeyFieldNames.toSet(),
    compareFields = compareFields,
    includeFields = includeFieldNames,
  )
}

private fun fetchLookupTable(
  ds: Datasource,
  con: Connection,
  dialect: Dialect,
  schema: String?,
  table: String,
  primaryKeyFieldNames: List<String>,
  compareFields: Set<String>,
  criteria: Set<Criteria>,
  cache: Cache,
): Result<Set<Row>> = runCatching {
  val includeFieldNames = primaryKeyFieldNames.toSet().union(compareFields)

  val tableDef = inspectTable(
    con = con,
    dialect = dialect,
    schema = schema,
    table = table,
    primaryKeyFieldNames = primaryKeyFieldNames,
    includeFieldNames = includeFieldNames,
    cache = cache,
  ).getOrThrow()

  if (tableDef == null) {
    throw KDAError.TableNotFound(schema = schema, table = table)
  } else {
    val srcLkpTable = tableDef.subset(includeFieldNames)
    val includeFields = tableDef.fields.filter { it.name in includeFieldNames }.toSet()
    val srcKeysSQL: String = ds.adapter.select(table = srcLkpTable, criteria = criteria)
    val result = ds.executor.fetchRows(sql = srcKeysSQL, fields = includeFields)
    result.toSet()
  }
}
