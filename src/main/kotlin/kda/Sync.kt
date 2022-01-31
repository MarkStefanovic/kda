package kda

import kda.adapter.selectAdapter
import kda.domain.Adapter
import kda.domain.Cache
import kda.domain.CopyTableResult
import kda.domain.Criteria
import kda.domain.DbDialect
import kda.domain.Field
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.SyncResult
import kda.domain.Table
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun sync(
  srcCon: Connection,
  dstCon: Connection,
  srcDialect: DbDialect,
  dstDialect: DbDialect,
  srcDbName: String,
  srcSchema: String?,
  srcTable: String,
  dstDbName: String,
  dstSchema: String?,
  dstTable: String,
  primaryKeyFieldNames: List<String>,
  cache: Cache,
  criteria: Criteria? = null,
  compareFields: Set<String>? = null,
  includeFields: Set<String>? = null,
  timestampFieldNames: Set<String> = setOf(),
  batchSize: Int = 1_000,
  showSQL: Boolean = false,
  queryTimeout: Duration = 30.minutes,
): SyncResult {
  val tables: CopyTableResult =
    copyTable(
      cache = cache,
      srcCon = srcCon,
      dstCon = dstCon,
      dstDialect = dstDialect,
      srcDbName = srcDbName,
      srcSchema = srcSchema,
      srcTable = srcTable,
      dstDbName = dstDbName,
      dstSchema = dstSchema,
      dstTable = dstTable,
      includeFields = includeFields,
      primaryKeyFieldNames = primaryKeyFieldNames,
    )

  val rowDiff: RowDiff = compareRows(
    srcCon = srcCon,
    dstCon = dstCon,
    srcDialect = srcDialect,
    dstDialect = dstDialect,
    srcDbName = srcDbName,
    srcSchema = srcSchema,
    srcTable = srcTable,
    dstDbName = dstDbName,
    dstSchema = dstSchema,
    dstTable = dstTable,
    primaryKeyFieldNames = primaryKeyFieldNames,
    cache = cache,
    criteria = criteria,
    compareFields = compareFields ?: emptySet(),
    includeFields = includeFields,
    timestampFieldNames = timestampFieldNames,
    batchSize = batchSize,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
  )

  val deleteKeys: Set<Row> =
    rowDiff
      .deleted
      .map { it.subset(tables.dstTable.primaryKeyFieldNames.toSet()) }
      .toSet()

  val srcAdapter = selectAdapter(
    dialect = srcDialect,
    con = srcCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
  )

  val dstAdapter = selectAdapter(
    dialect = dstDialect,
    con = dstCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
  )

  val rowsDeleted: Int = deleteRows(
    dstAdapter = dstAdapter,
    dstSchema = dstSchema,
    dstTable = tables.dstTable,
    deletedRows = deleteKeys,
    fields = tables.dstTable.primaryKeyFields.toSet(),
    chunkSize = batchSize,
  )

  val rowsUpserted: Int = upsertRows(
    srcAdapter = srcAdapter,
    dstAdapter = dstAdapter,
    srcSchema = srcSchema,
    dstSchema = dstSchema,
    srcTable = tables.srcTable,
    dstTable = tables.dstTable,
    primaryKeyFieldNames = tables.dstTable.primaryKeyFieldNames.toSet(),
    addedRows = rowDiff.added,
    updatedRows = rowDiff.updated,
    batchSize = batchSize,
  )

  return SyncResult(
    deleted = rowsDeleted,
    upserted = rowsUpserted,
  )
}

@ExperimentalStdlibApi
private fun deleteRows(
  dstAdapter: Adapter,
  dstTable: Table,
  dstSchema: String?,
  fields: Set<Field<*>>,
  deletedRows: Set<Row>,
  chunkSize: Int,
): Int =
  if (deletedRows.isEmpty()) {
    0
  } else {
    deletedRows.chunked(chunkSize) { keys ->
      dstAdapter.deleteRows(
        schema = dstSchema,
        table = dstTable.name,
        fields = fields,
        keys = keys.toSet(),
      )
    }.sum()
  }

@ExperimentalStdlibApi
private fun upsertRows(
  srcAdapter: Adapter,
  dstAdapter: Adapter,
  srcSchema: String?,
  dstSchema: String?,
  srcTable: Table,
  dstTable: Table,
  addedRows: Set<Row>,
  updatedRows: Set<Row>,
  batchSize: Int,
  primaryKeyFieldNames: Set<String>,
): Int =
  if (updatedRows.isEmpty() && addedRows.isEmpty()) {
    0
  } else {
    updatedRows.union(addedRows).chunked(batchSize) { keys ->
      val fullRows = srcAdapter.selectRows(
        schema = srcSchema,
        table = srcTable.name,
        keys = keys.toSet(),
        fields = srcTable.fields,
        batchSize = batchSize,
        orderBy = emptyList(),
      ).toSet()

      val primaryKeyFields: Set<Field<*>> =
        primaryKeyFieldNames.map { dstTable.field(it) }.toSet()

      val valueFields: Set<Field<*>> =
        dstTable.fields - primaryKeyFields

      dstAdapter.upsertRows(
        schema = dstSchema,
        table = dstTable.name,
        rows = fullRows,
        keyFields = primaryKeyFields,
        valueFields = valueFields,
      )
    }.sum()
  }
