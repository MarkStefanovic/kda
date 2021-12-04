package kda

import kda.adapter.selectAdapter
import kda.domain.Adapter
import kda.domain.Criteria
import kda.domain.DbDialect
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.Table
import java.sql.Connection

@ExperimentalStdlibApi
fun compareRows(
  srcCon: Connection,
  dstCon: Connection,
  cacheCon: Connection,
  srcDialect: DbDialect,
  dstDialect: DbDialect,
  cacheDialect: DbDialect,
  srcSchema: String?,
  srcTable: String,
  dstSchema: String?,
  dstTable: String,
  compareFields: Set<String>,
  primaryKeyFieldNames: List<String>,
  criteria: Criteria? = null,
  includeFields: Set<String>? = null,
  showSQL: Boolean = false,
  batchSize: Int = 1_000,
): RowDiff {
  val srcAdapter = selectAdapter(dialect = srcDialect, con = srcCon, showSQL = showSQL)

  val dstAdapter = selectAdapter(dialect = dstDialect, con = dstCon, showSQL = showSQL)

  val tables =
    copyTable(
      srcCon = srcCon,
      dstCon = dstCon,
      cacheCon = cacheCon,
      dstDialect = dstDialect,
      cacheDialect = cacheDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      dstSchema = dstSchema,
      dstTable = dstTable,
      includeFields = includeFields,
      primaryKeyFieldNames = primaryKeyFieldNames,
    )

  val srcRows: Set<Row> =
    fetchLookupTable(
      adapter = srcAdapter,
      primaryKeyFieldNames = primaryKeyFieldNames,
      compareFields = compareFields,
      criteria = criteria,
      schema = srcSchema,
      table = tables.srcTableDef,
      batchSize = batchSize,
    )

  val dstRows: Set<Row> =
    fetchLookupTable(
      adapter = dstAdapter,
      primaryKeyFieldNames = primaryKeyFieldNames,
      compareFields = compareFields,
      criteria = criteria,
      schema = dstSchema,
      table = tables.dstTableDef,
      batchSize = batchSize,
    )

  return kda.domain.compareRows(
    dstRows = dstRows,
    srcRows = srcRows,
    primaryKeyFieldNames = primaryKeyFieldNames.toSet(),
  )
}

@ExperimentalStdlibApi
private fun fetchLookupTable(
  adapter: Adapter,
  schema: String?,
  table: Table,
  primaryKeyFieldNames: List<String>,
  compareFields: Set<String>,
  criteria: Criteria?,
  batchSize: Int,
): Set<Row> {
  val includeFieldNames = primaryKeyFieldNames.toSet().union(compareFields)

  val includeFields = table.fields.filter { it.name in includeFieldNames }.toSet()

  return adapter
    .select(
      schema = schema,
      table = table.name,
      criteria = criteria,
      fields = includeFields,
      batchSize = batchSize,
      limit = null,
      orderBy = emptyList(),
    )
    .toSet()
}
