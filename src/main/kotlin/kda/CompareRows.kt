package kda

import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.Table
import java.sql.Connection

fun compareRows(
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
  compareFields: Set<String>,
  primaryKeyFieldNames: List<String>,
  criteria: Criteria = Criteria(emptySet()),
  includeFields: Set<String>? = null,
  showSQL: Boolean = false,
): Result<RowDiff> = runCatching {
  val src = datasource(con = srcCon, dialect = srcDialect)

  val dest = datasource(con = destCon, dialect = destDialect)

  val includeFieldNames = primaryKeyFieldNames.toSet().union(compareFields)

  val tables =
    copyTable(
      srcCon = srcCon,
      destCon = destCon,
      cacheCon = cacheCon,
      srcDialect = srcDialect,
      destDialect = destDialect,
      cacheDialect = cacheDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      includeFields = includeFields,
      primaryKeyFields = primaryKeyFieldNames,
    )
      .getOrThrow()

  val srcRows = fetchLookupTable(
    ds = src,
    primaryKeyFieldNames = primaryKeyFieldNames,
    compareFields = compareFields,
    criteria = criteria,
    tableDef = tables.srcTableDef,
    showSQL = showSQL,
  ).getOrThrow()

  val destRows = fetchLookupTable(
    ds = dest,
    primaryKeyFieldNames = primaryKeyFieldNames,
    compareFields = compareFields,
    criteria = criteria,
    tableDef = tables.destTableDef,
    showSQL = showSQL,
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
  tableDef: Table,
  primaryKeyFieldNames: List<String>,
  compareFields: Set<String>,
  criteria: Criteria,
  showSQL: Boolean,
): Result<Set<Row>> = runCatching {
  val includeFieldNames = primaryKeyFieldNames.toSet().union(compareFields)

  val srcLkpTable = tableDef.subset(includeFieldNames)

  val includeFields = tableDef.fields.filter { it.name in includeFieldNames }.toSet()

  val srcKeysSQL: String = ds.adapter.select(table = srcLkpTable, criteria = criteria)

  if (showSQL) {
    println(
      """Fetching lookup table for table ${tableDef.name}:
      |  $srcKeysSQL
    """.trimMargin()
    )
  }

  val result = ds.executor.fetchRows(sql = srcKeysSQL, fields = includeFields)

  result.toSet()
}
