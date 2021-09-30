package kda

import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.Field
import kda.domain.IndexedRows
import kda.domain.KDAError
import kda.domain.LatestTimestamp
import kda.domain.NullableLocalDateTimeType
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.SyncResult
import kda.domain.Table
import kda.domain.compareRows
import java.sql.Connection
import java.time.LocalDateTime

fun sync(
  srcCon: Connection,
  destCon: Connection,
  srcDialect: Dialect,
  destDialect: Dialect,
  srcSchema: String?,
  srcTable: String,
  destSchema: String?,
  destTable: String,
  primaryKeyFieldNames: List<String>,
  compareFields: Set<String>? = null,
  includeFields: Set<String>? = null,
  criteria: Set<Criteria> = emptySet(),
  cache: Cache = sqliteCache,
  timestampFieldNames: Set<String> = setOf(),
): Result<SyncResult> = runCatching {
  if (compareFields != null && compareFields.isEmpty()) {
    throw KDAError.InvalidArgument(
      errorMessage = "If a value is provided, then it must contain at least one field name.",
      argumentName = "compareFields",
      argumentValue = compareFields,
    )
  }

  val src = datasource(con = srcCon, dialect = srcDialect)

  val dest = datasource(con = destCon, dialect = destDialect)

  val tables =
    copyTable(
      srcCon = srcCon,
      destCon = destCon,
      srcDialect = srcDialect,
      destDialect = destDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      includeFields = includeFields,
      primaryKeyFields = primaryKeyFieldNames,
      cache = cache,
    ).getOrThrow()

  val fieldNames = tables.srcTableDef.fields.map { it.name }.toSet()

  val compareFieldNamesFinal: Set<String> =
    if (compareFields == null) {
      fieldNames.minus(tables.srcTableDef.primaryKeyFieldNames)
    } else {
      fieldNames.filter { fldName -> fldName in compareFields }.toSet()
    }

  val pkFields = tables.srcTableDef.primaryKeyFieldNames.toSet()

  val lkpTableFieldNames = pkFields.union(compareFieldNamesFinal).union(timestampFieldNames)

  val lkpTableFields = tables.srcTableDef.fields.filter { fld -> fld.name in lkpTableFieldNames }.toSet()

  val fullCriteria =
    getFullCriteria(
      ds = dest,
      table = tables.destTableDef,
      tsFieldNames = timestampFieldNames,
      cache = cache,
      criteria = criteria,
    ).getOrThrow()

  val srcLkpTable = tables.srcTableDef.subset(fieldNames = lkpTableFieldNames)
  val srcKeysSQL = src.adapter.select(table = srcLkpTable, criteria = fullCriteria)
  val srcLkpRows = src.executor.fetchRows(sql = srcKeysSQL, fields = lkpTableFields)

  val destLkpTable = tables.destTableDef.subset(fieldNames = lkpTableFieldNames)
  val destKeysSQL = dest.adapter.select(table = destLkpTable, criteria = fullCriteria)
  val destLkpRows = dest.executor.fetchRows(sql = destKeysSQL, fields = lkpTableFields)

  val rowDiff: RowDiff =
    compareRows(
      dest = destLkpRows,
      src = srcLkpRows,
      primaryKeyFields = pkFields,
      compareFields = compareFieldNamesFinal,
      includeFields = lkpTableFieldNames,
    )

  deleteRows(
    dest = dest,
    destTableDef = tables.destTableDef,
    deletedRows = rowDiff.deleted,
  ).getOrThrow()

  if (fullCriteria.isEmpty()) {
    addRows(
      src = src,
      dest = dest,
      srcTableDef = tables.srcTableDef,
      destTableDef = tables.destTableDef,
      addedRows = rowDiff.added,
    ).getOrThrow()

    updateRows(
      src = src,
      dest = dest,
      srcTableDef = tables.srcTableDef,
      destTableDef = tables.destTableDef,
      updatedRows = rowDiff.updated,
    ).getOrThrow()

    if (timestampFieldNames.isNotEmpty()) {
      updateLatestTimestamps(
        timestampFieldNames = timestampFieldNames,
        destRows = destLkpRows,
        destSchema = destSchema,
        destTable = destTable,
        cache = cache,
      ).getOrThrow()
    }
  } else {
    upsertRows(
      src = src,
      dest = dest,
      srcTableDef = tables.srcTableDef,
      destTableDef = tables.destTableDef,
      addedRows = rowDiff.added,
      updatedRows = rowDiff.updated,
    ).getOrThrow()

    if (timestampFieldNames.isNotEmpty()) {
      updateLatestTimestamps(
        timestampFieldNames = timestampFieldNames,
        destRows = destLkpRows,
        destSchema = destSchema,
        destTable = destTable,
        cache = cache,
      ).getOrThrow()
    }
  }
  SyncResult(
    srcTableDef = tables.srcTableDef,
    destTableDef = tables.destTableDef,
    added = rowDiff.added,
    deleted = rowDiff.deleted,
    updated = rowDiff.updated,
  )
}

private fun addRows(
  src: Datasource,
  dest: Datasource,
  srcTableDef: Table,
  destTableDef: Table,
  addedRows: IndexedRows
): Result<Int> = runCatching {
  if (addedRows.keys.isNotEmpty()) {
    val selectSQL: String =
      src.adapter.selectKeys(table = srcTableDef, primaryKeyValues = addedRows.keys)
    val newRows: Set<Row> = src.executor.fetchRows(sql = selectSQL, fields = srcTableDef.fields)
    val insertSQL: String = dest.adapter.add(table = destTableDef, rows = newRows)
    dest.executor.execute(sql = insertSQL)
  }
  addedRows.count()
}

private fun deleteRows(
  dest: Datasource,
  destTableDef: Table,
  deletedRows: IndexedRows
): Result<Int> = runCatching {
  if (deletedRows.keys.isNotEmpty()) {
    val deleteSQL: String =
      dest.adapter.deleteKeys(table = destTableDef, primaryKeyValues = deletedRows.keys)
    dest.executor.execute(sql = deleteSQL)
  }
  deletedRows.count()
}

private fun upsertRows(
  src: Datasource,
  dest: Datasource,
  srcTableDef: Table,
  destTableDef: Table,
  addedRows: IndexedRows,
  updatedRows: IndexedRows,
): Result<Int> = runCatching {
  if (addedRows.keys.isNotEmpty()) {
    val selectSQL: String =
      src.adapter.selectKeys(
        table = srcTableDef, primaryKeyValues = addedRows.keys + updatedRows.keys
      )
    val newRows: Set<Row> = src.executor.fetchRows(sql = selectSQL, fields = srcTableDef.fields)
    val upsertSQL: String = dest.adapter.merge(table = destTableDef, rows = newRows)
    dest.executor.execute(sql = upsertSQL)
  }
  addedRows.count()
}

private fun updateRows(
  src: Datasource,
  dest: Datasource,
  srcTableDef: Table,
  destTableDef: Table,
  updatedRows: IndexedRows
): Result<Int> = runCatching {
  if (updatedRows.isNotEmpty()) {
    val selectSQL = src.adapter.selectKeys(table = srcTableDef, primaryKeyValues = updatedRows.keys)
    val fullRows: Set<Row> = src.executor.fetchRows(sql = selectSQL, fields = srcTableDef.fields)
    val updateSQL = dest.adapter.update(table = destTableDef, rows = fullRows)
    dest.executor.execute(sql = updateSQL)
  }
  updatedRows.count()
}

private fun getFullCriteria(
  ds: Datasource,
  table: Table,
  tsFieldNames: Set<String>,
  cache: Cache,
  criteria: Set<Criteria>,
): Result<Set<Criteria>> = runCatching {
  if (tsFieldNames.isEmpty()) {
    criteria
  } else {
    val cachedTimestamps = cache.latestTimestamps(
      schema = table.schema ?: "",
      table = table.name,
    ).getOrThrow()

    val tsCriteria = if (cachedTimestamps.isEmpty()) {
      val sql = ds.adapter.selectMaxValues(table = table, fieldNames = tsFieldNames)
      val tsFields =
        tsFieldNames
          .map { fld -> Field(name = fld, dataType = NullableLocalDateTimeType) }
          .toSet()
      val row = ds.executor.fetchRows(sql = sql, fields = tsFields).first()
      val timestamps: Set<LatestTimestamp> =
        tsFieldNames
          .map { fld ->
            LatestTimestamp(
              fieldName = fld, timestamp = row.value(fld).value as LocalDateTime?
            )
          }
          .toSet()
      cache.addLatestTimestamp(
        schema = table.schema ?: "",
        table = table.name,
        timestamps = timestamps,
      )
      timestamps.map { Criteria(setOf(it.toPredicate())) }.toSet()
    } else {
      setOf()
    }
    criteria + tsCriteria
  }
}

private fun updateLatestTimestamps(
  timestampFieldNames: Set<String>,
  destRows: Set<Row>,
  destSchema: String?,
  destTable: String,
  cache: Cache,
): Result<Unit> = runCatching {
  if (timestampFieldNames.isNotEmpty()) {
    val latestTimestamps =
      timestampFieldNames
        .map { fld ->
          val latestTimestamp =
            destRows.mapNotNull { it.value(fld).value as LocalDateTime? }.maxOrNull()
          LatestTimestamp(fieldName = fld, timestamp = latestTimestamp)
        }
        .toSet()
    cache.addLatestTimestamp(
      schema = destSchema ?: "",
      table = destTable,
      timestamps = latestTimestamps,
    )
  }
}
