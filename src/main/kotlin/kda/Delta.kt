package kda

import kda.adapter.selectAdapter
import kda.domain.Adapter
import kda.domain.Cache
import kda.domain.CopyTableResult
import kda.domain.Criteria
import kda.domain.DataType
import kda.domain.DbDialect
import kda.domain.DeltaResult
import kda.domain.Field
import kda.domain.Index
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.Table
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun delta(
  srcCon: Connection,
  dstCon: Connection,
  deltaCon: Connection = dstCon,
  srcDialect: DbDialect,
  dstDialect: DbDialect,
  deltaDialect: DbDialect = dstDialect,
  deltaDbName: String,
  srcDbName: String,
  srcSchema: String?,
  srcTable: String,
  dstDbName: String,
  dstSchema: String?,
  dstTable: String,
  deltaSchema: String? = dstSchema,
  deltaTable: String = dstTable + "_delta",
  primaryKeyFieldNames: List<String>,
  cache: Cache,
  srcCriteria: Criteria? = null,
  dstCriteria: Criteria? = null,
  compareFields: Set<String>? = null,
  includeFields: Set<String>? = null,
  timestampFieldNames: Set<String> = setOf(),
  batchSize: Int = 1_000,
  showSQL: Boolean = false,
  queryTimeout: Duration = 30.minutes,
  timestampResolution: ChronoUnit = ChronoUnit.MILLIS,
): DeltaResult {
  val batchTs = OffsetDateTime.now(ZoneId.of("Etc/UTC"))

  val tables: CopyTableResult = copyTable(
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
    timestampResolution = timestampResolution,
  )

  val rowDiff: RowDiff = compareRows(
    srcCon = srcCon,
    dstCon = dstCon,
    srcDialect = srcDialect,
    dstDialect = dstDialect,
    srcDbName = dstDbName,
    srcSchema = srcSchema,
    srcTable = srcTable,
    dstDbName = dstDbName,
    dstSchema = dstSchema,
    dstTable = dstTable,
    primaryKeyFieldNames = primaryKeyFieldNames,
    cache = cache,
    srcCriteria = srcCriteria,
    dstCriteria = dstCriteria,
    compareFields = compareFields ?: emptySet(),
    includeFields = includeFields,
    timestampFieldNames = timestampFieldNames,
    batchSize = batchSize,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  )

  val deltaTableDef = Table(
    name = deltaTable,
    fields =
    tables.dstTable.fields +
      setOf(
        Field(name = "kda_ts", DataType.timestampUTC(0)),
        Field(name = "kda_op", DataType.text(1)),
      ),
    primaryKeyFieldNames = tables.dstTable.primaryKeyFieldNames + "kda_ts",
  )

  val deltaAdapter = selectAdapter(
    dialect = deltaDialect,
    con = deltaCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  )

  if (
    !cache.tableExists(
      con = deltaCon,
      dbName = deltaDbName,
      schema = deltaSchema,
      table = deltaTable,
    )
  ) {
    deltaAdapter.createTable(
      schema = dstSchema,
      table = deltaTableDef,
    )
    deltaAdapter.createIndex(
      schema = deltaSchema,
      table = deltaTable,
      index = Index(tableName = deltaTable, fields = listOf("kda_op" to true, "kda_ts" to false))
    )
  }

  val srcAdapter = selectAdapter(
    dialect = srcDialect,
    con = srcCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  )

  val rowsAdded: Int = addRowsFromSource(
    deltaAdapter = deltaAdapter,
    srcAdapter = srcAdapter,
    deltaSchema = deltaSchema,
    srcSchema = srcSchema,
    deltaTable = deltaTableDef,
    srcTable = tables.srcTable,
    keys = rowDiff.added,
    op = "I",
    chunkSize = batchSize,
    batchTimestamp = batchTs,
  )

  val rowsUpdated: Int = addRowsFromSource(
    deltaAdapter = deltaAdapter,
    srcAdapter = srcAdapter,
    deltaSchema = deltaSchema,
    srcSchema = srcSchema,
    deltaTable = deltaTableDef,
    srcTable = tables.srcTable,
    keys = rowDiff.updated,
    op = "U",
    chunkSize = batchSize,
    batchTimestamp = batchTs
  )

  val dstAdapter = selectAdapter(
    dialect = dstDialect,
    con = dstCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  )

  val rowsDeleted: Int = addDeletedRows(
    deltaAdapter = deltaAdapter,
    dstAdapter = dstAdapter,
    deltaSchema = deltaSchema,
    dstSchema = dstSchema,
    deltaTable = deltaTableDef,
    dstTable = tables.dstTable,
    keys = rowDiff.deleted,
    chunkSize = batchSize,
    batchTimestamp = batchTs,
  )

  return DeltaResult(
    added = rowsAdded,
    deleted = rowsDeleted,
    updated = rowsUpdated,
  )
}

@ExperimentalStdlibApi
private fun addRowsFromSource(
  deltaAdapter: Adapter,
  srcAdapter: Adapter,
  deltaSchema: String?,
  srcSchema: String?,
  deltaTable: Table,
  srcTable: Table,
  keys: Set<Row>,
  op: String,
  chunkSize: Int,
  batchTimestamp: OffsetDateTime,
): Int =
  if (keys.isEmpty()) {
    0
  } else {
    val fullRows = srcAdapter.selectRows(
      schema = srcSchema,
      table = srcTable.name,
      keys = keys.toSet(),
      fields = srcTable.fields,
      batchSize = chunkSize,
      orderBy = emptyList(),
    ).toSet()

    val extendedRows = fullRows.map {
      it.add("kda_ts" to batchTimestamp, "kda_op" to op)
    }
    extendedRows.chunked(chunkSize) { batch ->
      deltaAdapter.addRows(
        schema = deltaSchema,
        table = deltaTable.name,
        rows = batch,
        fields = deltaTable.fields,
      )
    }.sum()
  }

@ExperimentalStdlibApi
private fun addDeletedRows(
  deltaAdapter: Adapter,
  dstAdapter: Adapter,
  deltaSchema: String?,
  dstSchema: String?,
  deltaTable: Table,
  dstTable: Table,
  keys: Set<Row>,
  chunkSize: Int,
  batchTimestamp: OffsetDateTime,
): Int =
  if (keys.isEmpty()) {
    0
  } else {
    val fullRows = dstAdapter.selectRows(
      schema = dstSchema,
      table = dstTable.name,
      keys = keys.toSet(),
      fields = dstTable.fields,
      batchSize = chunkSize,
      orderBy = emptyList(),
    ).toSet()

    val extendedRows = fullRows.map {
      it.add("kda_ts" to batchTimestamp, "kda_op" to "D")
    }
    extendedRows.chunked(chunkSize) { batch ->
      deltaAdapter.addRows(
        schema = deltaSchema,
        table = deltaTable.name,
        rows = batch,
        fields = deltaTable.fields,
      )
    }.sum()
  }
