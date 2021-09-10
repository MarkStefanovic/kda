package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.CopyTableResult
import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.Field
import kda.domain.IndexedRows
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
  criteria: List<Criteria> = emptyList(),
  cache: Cache = DbCache(),
  timestampFieldNames: Set<String> = setOf(),
): SyncResult =
  try {
    if (compareFields != null && compareFields.isEmpty()) {
      SyncResult.Error.InvalidArgument(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "If a value is provided, then it must contain at least one field name.",
        originalError = null,
        argumentName = "compareFields",
        argumentValue = compareFields,
      )
    } else {
      val src: Datasource =
        when (srcDialect) {
          Dialect.HortonworksHive -> hiveDatasource(con = srcCon)
          Dialect.PostgreSQL -> pgDatasource(con = srcCon)
        }

      val dest: Datasource =
        when (destDialect) {
          Dialect.HortonworksHive -> hiveDatasource(con = destCon)
          Dialect.PostgreSQL -> pgDatasource(con = destCon)
        }

      when (
        val copySrcResult =
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
          )
      ) {
        is CopyTableResult.Error -> {
          SyncResult.Error.CopyTableFailed(
            srcSchema = srcSchema,
            srcTable = srcTable,
            destSchema = destSchema,
            destTable = destTable,
            errorMessage = copySrcResult.errorMessage,
            originalError = copySrcResult.originalError,
          )
        }
        is CopyTableResult.Success -> {
          val fieldNames = copySrcResult.srcTableDef.fields.map { it.name }.toSet()
          val compareFieldNamesFinal: Set<String> =
            if (compareFields == null) {
              fieldNames.minus(copySrcResult.srcTableDef.primaryKeyFieldNames)
            } else {
              fieldNames.filter { fldName -> fldName in compareFields }.toSet()
            }
          val pkFields = copySrcResult.srcTableDef.primaryKeyFieldNames.toSet()
          val lkpTableFieldNames = pkFields.union(compareFieldNamesFinal)
          val lkpTableFields = copySrcResult.srcTableDef.fields.filter { fld -> fld.name in lkpTableFieldNames }.toSet()

          val fullCriteria: List<Criteria> =
            if (timestampFieldNames.isEmpty()) {
              criteria
            } else {
              val cachedLatestTimestamps =
                cache.latestTimestamps(schema = srcSchema ?: "", table = srcTable)
              if (cachedLatestTimestamps == null) {
                val sql = src.adapter.selectMaxValues(copySrcResult.srcTableDef, timestampFieldNames)
                val tsFields =
                  timestampFieldNames
                    .map { fld -> Field(name = fld, dataType = NullableLocalDateTimeType) }
                    .toSet()
                val row = src.executor.fetchRows(sql = sql, fields = tsFields).first()
                val timestamps: List<LatestTimestamp> =
                  timestampFieldNames.map { fld ->
                    LatestTimestamp(
                      fieldName = fld, timestamp = row.value(fld).value as LocalDateTime?
                    )
                  }
                cache.addLatestTimestamp(
                  schema = srcSchema ?: "",
                  table = srcTable,
                  timestamps = timestamps.toSet(),
                )
                val tsCriteria = timestamps.map { Criteria(listOf(it.toPredicate())) }
                criteria + tsCriteria
              } else {
                criteria + cachedLatestTimestamps
              }
            }

          val srcLkpTable = copySrcResult.srcTableDef.subset(fieldNames = lkpTableFieldNames)
          val srcKeysSQL: String = src.adapter.select(table = srcLkpTable, criteria = fullCriteria)
          val srcLkpRows: Set<Row> = src.executor.fetchRows(sql = srcKeysSQL, fields = lkpTableFields)

          val destLkpTable = copySrcResult.destTableDef.subset(fieldNames = lkpTableFieldNames)
          val destKeysSQL: String = dest.adapter.select(table = destLkpTable, criteria = fullCriteria)
          val destLkpRows = dest.executor.fetchRows(sql = destKeysSQL, fields = lkpTableFields)

          try {
            val rowDiff: RowDiff = compareRows(
              old = destLkpRows,
              new = srcLkpRows,
              primaryKeyFields = pkFields,
              compareFields = compareFieldNamesFinal,
              includeFields = lkpTableFieldNames,
            )

            val (addRowsError, addedRows) = addRows(
              src = src,
              dest = dest,
              srcTableDef = copySrcResult.srcTableDef,
              destTableDef = copySrcResult.destTableDef,
              addedRows = rowDiff.added,
            )

            if (addRowsError != null) {
              addRowsError
            } else {
              val (deleteRowsError, deletedRows) = deleteRows(
                dest = dest,
                srcTableDef = copySrcResult.srcTableDef,
                destTableDef = copySrcResult.destTableDef,
                deletedRows = rowDiff.deleted,
              )
              if (deleteRowsError != null) {
                deleteRowsError
              } else {
                val (updatedRowsError, updatedRows) = updateRows(
                  src = src,
                  dest = dest,
                  srcTableDef = copySrcResult.srcTableDef,
                  destTableDef = copySrcResult.destTableDef,
                  updatedRows = rowDiff.updated,
                )
                updatedRowsError ?: SyncResult.Success(
                  srcSchema = srcSchema,
                  srcTable = srcTable,
                  destSchema = destSchema,
                  destTable = destTable,
                  srcTableDef = copySrcResult.srcTableDef,
                  destTableDef = copySrcResult.destTableDef,
                  added = addedRows,
                  deleted = deletedRows,
                  updated = updatedRows,
                )
              }
            }
          } catch (e: Exception) {
            SyncResult.Error.RowComparisonFailed(
              srcSchema = srcSchema,
              srcTable = srcTable,
              destSchema = destSchema,
              destTable = destTable,
              errorMessage = "An error occurred while executing compareRows: $e",
              originalError = null,
              srcRows = srcLkpRows,
              destRows = destLkpRows,
              pkFields = pkFields,
              compareFields = compareFieldNamesFinal,
              includeFields = lkpTableFieldNames,
            )
          }
        }
      }
    }
  } catch (e: Exception) {
    SyncResult.Error.Unexpected(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      errorMessage = "An unexpected error occurred while syncing: ${e.message}",
      originalError = e,
    )
  }

private fun addRows(
  src: Datasource,
  dest: Datasource,
  srcTableDef: Table,
  destTableDef: Table,
  addedRows: IndexedRows
): Pair<SyncResult.Error?, Int> =
  try {
    if (addedRows.keys.isNotEmpty()) {
      val selectSQL: String =
        src.adapter.selectKeys(table = srcTableDef, primaryKeyValues = addedRows.keys)
      val newRows: Set<Row> =
        src.executor.fetchRows(sql = selectSQL, fields = srcTableDef.fields)
      val insertSQL: String = dest.adapter.add(table = destTableDef, rows = newRows)
      dest.executor.execute(sql = insertSQL)
    }
    null to addedRows.count()
  } catch (e: Exception) {
    SyncResult.Error.AddRowsFailed(
      srcSchema = srcTableDef.schema,
      srcTable = srcTableDef.name,
      destSchema = destTableDef.schema,
      destTable = destTableDef.name,
      errorMessage = "An error occurred while adding rows: $e",
      originalError = e,
      rows = addedRows.values.toSet(),
    ) to 0
  }

private fun deleteRows(
  dest: Datasource,
  srcTableDef: Table,
  destTableDef: Table,
  deletedRows: IndexedRows
): Pair<SyncResult.Error?, Int> =
  try {
    if (deletedRows.keys.isNotEmpty()) {
      val deleteSQL: String =
        dest.adapter.deleteKeys(table = destTableDef, primaryKeyValues = deletedRows.keys)
      dest.executor.execute(sql = deleteSQL)
    }
    null to deletedRows.count()
  } catch (e: Exception) {
    SyncResult.Error.DeleteRowsFailed(
      srcSchema = srcTableDef.schema,
      srcTable = srcTableDef.name,
      destSchema = destTableDef.schema,
      destTable = destTableDef.name,
      errorMessage = "An error occurred while deleting rows: $e",
      originalError = e,
      rows = deletedRows.values.toSet(),
    ) to 0
  }

private fun updateRows(
  src: Datasource,
  dest: Datasource,
  srcTableDef: Table,
  destTableDef: Table,
  updatedRows: IndexedRows
): Pair<SyncResult.Error?, Int> =
  try {
    if (updatedRows.isNotEmpty()) {
      val selectSQL: String =
        src.adapter.selectKeys(table = srcTableDef, primaryKeyValues = updatedRows.keys)
      val fullRows: Set<Row> =
        src.executor.fetchRows(sql = selectSQL, fields = srcTableDef.fields)
      val updateSQL: String = dest.adapter.update(table = destTableDef, rows = fullRows)
      dest.executor.execute(sql = updateSQL)
    }
    null to updatedRows.count()
  } catch (e: Exception) {
    SyncResult.Error.UpdateRowsFailed(
      srcSchema = srcTableDef.schema,
      srcTable = srcTableDef.name,
      destSchema = destTableDef.schema,
      destTable = destTableDef.name,
      errorMessage = "An error occurred while updating rows: $e",
      originalError = e,
      rows = updatedRows.values.toSet(),
    ) to 0
  }
