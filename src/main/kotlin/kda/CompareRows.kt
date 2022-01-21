package kda

import kda.adapter.selectAdapter
import kda.domain.Adapter
import kda.domain.Criteria
import kda.domain.DbDialect
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.Table
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.minutes

@ExperimentalTime
@ExperimentalStdlibApi
fun compareRows(
  srcCon: Connection,
  dstCon: Connection,
  cacheCon: Connection,
  srcDialect: DbDialect,
  dstDialect: DbDialect,
  cacheDialect: DbDialect,
  cacheSchema: String?,
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
  queryTimeout: Duration = Duration.minutes(30),
): RowDiff {
  val srcAdapter = selectAdapter(dialect = srcDialect, con = srcCon, showSQL = showSQL, queryTimeout = queryTimeout)

  val dstAdapter = selectAdapter(dialect = dstDialect, con = dstCon, showSQL = showSQL, queryTimeout = queryTimeout)

  val tables =
    copyTable(
      srcCon = srcCon,
      dstCon = dstCon,
      cacheCon = cacheCon,
      dstDialect = dstDialect,
      cacheDialect = cacheDialect,
      cacheSchema = cacheSchema,
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
      table = tables.srcTable,
      batchSize = batchSize,
    )

  val dstRows: Set<Row> =
    fetchLookupTable(
      adapter = dstAdapter,
      primaryKeyFieldNames = primaryKeyFieldNames,
      compareFields = compareFields,
      criteria = criteria,
      schema = dstSchema,
      table = tables.dstTable,
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
