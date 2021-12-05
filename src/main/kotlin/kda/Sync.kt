@file:OptIn(ExperimentalStdlibApi::class)

package kda

import kda.adapter.selectAdapter
import kda.adapter.sqlite.SQLiteCache
import kda.adapter.where
import kda.domain.Adapter
import kda.domain.Cache
import kda.domain.CopyTableResult
import kda.domain.Criteria
import kda.domain.DbDialect
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Operator
import kda.domain.Predicate
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.SyncResult
import kda.domain.Table
import kda.domain.compareRows
import java.sql.Connection
import java.time.LocalDateTime
import java.time.ZoneOffset

@ExperimentalStdlibApi
fun sync(
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
  primaryKeyFieldNames: List<String>,
  criteria: Criteria? = null,
  compareFields: Set<String>? = null,
  includeFields: Set<String>? = null,
  timestampFieldNames: Set<String> = setOf(),
  batchSize: Int = 1_000,
  showSQL: Boolean = false,
): SyncResult {
  if (compareFields != null && compareFields.isEmpty()) {
    throw KDAError.InvalidArgument(
      errorMessage = "If a value is provided, then it must contain at least one field name.",
      argumentName = "compareFields",
      argumentValue = compareFields,
    )
  }

  val srcAdapter = selectAdapter(dialect = srcDialect, con = srcCon, showSQL = showSQL)

  val dstAdapter = selectAdapter(dialect = dstDialect, con = dstCon, showSQL = showSQL)

  val cache: Cache = when (cacheDialect) {
    DbDialect.HH -> TODO()
    DbDialect.MSSQL -> TODO()
    DbDialect.PostgreSQL -> TODO()
    DbDialect.SQLite -> SQLiteCache(con = cacheCon, showSQL = showSQL)
  }

  val tables: CopyTableResult =
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

  val fieldNames = tables.srcTable.fields.map { it.name }.toSet()

  val compareFieldNamesFinal: Set<String> =
    if (compareFields == null) {
      fieldNames.minus(tables.srcTable.primaryKeyFieldNames.toSet())
    } else {
      fieldNames.filter { fldName -> fldName in compareFields }.toSet()
    }

  val pkFields = tables.srcTable.primaryKeyFieldNames.toSet()

  val lkpTableFieldNames = pkFields.union(compareFieldNamesFinal).union(timestampFieldNames)

  val lkpTableFields =
    tables.srcTable.fields.filter { fld -> fld.name in lkpTableFieldNames }.toSet()

  val fullCriteria = getFullCriteria(
    dstDialect = dstDialect,
    dstSchema = dstSchema,
    dstTable = tables.dstTable,
    tsFieldNames = timestampFieldNames,
    cache = cache,
    criteria = criteria,
  )

  val srcLkpRows: Set<Row> = srcAdapter.select(
    fields = lkpTableFields,
    schema = srcSchema,
    table = srcTable,
    batchSize = batchSize,
    criteria = fullCriteria,
    limit = null,
    orderBy = listOf(),
  ).toSet()

  val dstLkpRows: Set<Row> = dstAdapter.select(
    fields = lkpTableFields,
    schema = dstSchema,
    table = dstTable,
    batchSize = batchSize,
    criteria = fullCriteria,
    limit = null,
    orderBy = listOf(),
  ).toSet()

  val rowDiff: RowDiff =
    compareRows(
      dstRows = dstLkpRows,
      srcRows = srcLkpRows,
      primaryKeyFieldNames = pkFields,
    )

  val deleteKeys: Set<Row> =
    rowDiff
      .deleted
      .map { it.subset(tables.dstTable.primaryKeyFieldNames.toSet()) }
      .toSet()

  deleteRows(
    dstAdapter = dstAdapter,
    dstSchema = dstSchema,
    dstTable = tables.dstTable,
    deletedRows = deleteKeys,
    fields = tables.dstTable.primaryKeyFields.toSet(),
    chunkSize = batchSize,
  )

  upsertRows(
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

  if (timestampFieldNames.isNotEmpty()) {
    updateLatestTimestamps(
      timestampFieldNames = timestampFieldNames,
      addedKeys = rowDiff.added,
      updatedKeys = rowDiff.updated,
      dstSchema = dstSchema,
      dstTable = dstTable,
      cache = cache,
    )
  }

  return SyncResult(
    deleted = rowDiff.deleted.count(),
    upserted = rowDiff.added.count() + rowDiff.updated.count(),
  )
}

private fun deleteRows(
  dstAdapter: Adapter,
  dstTable: Table,
  dstSchema: String?,
  fields: Set<Field<*>>,
  deletedRows: Set<Row>,
  chunkSize: Int,
) {
  if (deletedRows.isNotEmpty()) {
    deletedRows.chunked(chunkSize) { keys ->
      dstAdapter.deleteRows(
        schema = dstSchema,
        table = dstTable.name,
        fields = fields,
        keys = keys.toSet(),
      )
    }
  }
}

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
) {
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
  }
}

@ExperimentalStdlibApi
internal fun getFullCriteria(
  dstDialect: DbDialect,
  dstSchema: String?,
  dstTable: Table,
  tsFieldNames: Set<String>,
  cache: Cache,
  criteria: Criteria?,
): Criteria? =
  if (tsFieldNames.isEmpty()) {
    criteria
  } else {
    val cachedTimestamps: Map<String, LocalDateTime?> =
      tsFieldNames
        .associateWith { fieldName ->
          cache.getTimestamp(schema = dstSchema, table = dstTable.name, fieldName = fieldName)
        }

    val tsFields: Set<Field<*>> =
      tsFieldNames
        .map { fieldName ->
          dstTable.field(fieldName)
        }
        .toSet()

    var tsCriteria: Criteria = where(dstDialect)
    tsFields.forEach { field ->
      val ts = cachedTimestamps[field.name]
      if (ts != null) {
        tsCriteria = tsCriteria.or(Predicate(field = field, operator = Operator.GreaterThan, value = ts))
      }
    }

    if (tsCriteria.isEmpty) {
      criteria
    } else {
      criteria?.and(tsCriteria) ?: tsCriteria
    }
  }

private fun updateLatestTimestamps(
  cache: Cache,
  dstSchema: String?,
  dstTable: String,
  timestampFieldNames: Set<String>,
  addedKeys: Set<Row>,
  updatedKeys: Set<Row>,
) {
  if (timestampFieldNames.isNotEmpty()) {
    timestampFieldNames.forEach { fieldName ->
      if (addedKeys.isNotEmpty()) {
        if (!addedKeys.first().value.containsKey(fieldName)) {
          throw KDAError.FieldNotFound(fieldName = fieldName, availableFieldNames = addedKeys.first().value.keys)
        }
      }

      if (updatedKeys.isNotEmpty()) {
        if (!updatedKeys.first().value.containsKey(fieldName)) {
          throw KDAError.FieldNotFound(fieldName = fieldName, availableFieldNames = updatedKeys.first().value.keys)
        }
      }
    }

    val allKeys = addedKeys + updatedKeys

    timestampFieldNames.forEach { timestampFieldName ->
      val ts: LocalDateTime? =
        allKeys
          .maxByOrNull { row ->
            (row.value[timestampFieldName] as LocalDateTime?)
              ?.toEpochSecond(ZoneOffset.UTC)
              ?: LocalDateTime.MIN.toEpochSecond(ZoneOffset.UTC)
          }
          ?.value
          ?.get(timestampFieldName) as LocalDateTime?

      cache.addTimestamp(
        schema = dstSchema,
        table = dstTable,
        fieldName = timestampFieldName,
        ts = ts,
      )
    }
  }
}
