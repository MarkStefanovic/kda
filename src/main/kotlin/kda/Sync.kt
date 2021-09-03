package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.CopyTableResult
import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.Field
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.SyncResult
import kda.domain.Table
import kda.domain.compareRows
import java.sql.Connection

fun sync(
  srcCon: Connection,
  destCon: Connection,
  srcDialect: Dialect,
  destDialect: Dialect,
  srcSchema: String?,
  srcTable: String,
  destSchema: String?,
  destTable: String,
  compareFields: Set<String>? = null,
  primaryKeyFieldNames: List<String>? = null,
  includeFields: Set<String>? = null,
  maxFloatDigits: Int = 5,
  criteria: List<Criteria> = emptyList(),
): SyncResult {
  try {
    if (compareFields != null && compareFields.isEmpty())
      return SyncResult.Error.InvalidArgument(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "If a value is provided, then it must contain at least one field name.",
        originalError = null,
        argumentName = "compareFields",
        argumentValue = compareFields,
      )

    if (primaryKeyFieldNames != null && primaryKeyFieldNames.isEmpty())
      return SyncResult.Error.InvalidArgument(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "If a value is provided, then it must contain at least one field name.",
        originalError = null,
        argumentName = "primaryKeyFieldNames",
        argumentValue = primaryKeyFieldNames,
      )

    if (includeFields != null && includeFields.isEmpty())
      return SyncResult.Error.InvalidArgument(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "If a value is provided, then it must contain at least one field name.",
        originalError = null,
        argumentName = "includeFields",
        argumentValue = includeFields,
      )

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

    val actualSrcTableDef: Table = try {
      src.inspector.inspectTable(
        schema = srcSchema,
        table = srcTable,
        maxFloatDigits = maxFloatDigits,
        primaryKeyFieldNames = primaryKeyFieldNames,
      )
    } catch (e: Exception) {
      return SyncResult.Error.InspectTableFailed(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "inspectTable(schema = $srcSchema, table = $srcTable, maxFloatingDigits = $maxFloatDigits) failed with the following error: ${e.message}",
        originalError = e,
      )
    }

    val pkFieldsFinal: List<String> =
      if (primaryKeyFieldNames == null) {
        actualSrcTableDef.primaryKeyFieldNames
      } else {
        val missingFields: Set<String> =
          primaryKeyFieldNames.toSet().minus(actualSrcTableDef.sortedFieldNames.toSet())
        if (missingFields.isNotEmpty()) {
          val pkFieldNamesCSV = primaryKeyFieldNames.joinToString(", ")
          val missingFieldsCSV = missingFields.joinToString(", ")
          val fieldNameCSV = actualSrcTableDef.sortedFieldNames.joinToString(", ")
          val errorMessage =
            "The following primary key field(s) were specified: $pkFieldNamesCSV.  " +
              "However, the table does not include the following fields: $missingFieldsCSV.  " +
              "The table includes the following fields: $fieldNameCSV"
          return SyncResult.Error.InvalidArgument(
            srcSchema = srcSchema,
            srcTable = srcTable,
            destSchema = destSchema,
            destTable = destTable,
            errorMessage = errorMessage,
            originalError = null,
            argumentName = "primaryKeyFieldNames",
            argumentValue = primaryKeyFieldNames,
          )
        }
        primaryKeyFieldNames
      }

    val fieldsFinal: Set<Field> =
      if (includeFields == null) {
        actualSrcTableDef.fields
      } else {
        val missingFields: Set<String> = includeFields.minus(pkFieldsFinal.toSet())
        if (missingFields.isEmpty()) {
          actualSrcTableDef.fields.filter { fld -> fld.name in includeFields }.toSet()
        } else {
          val includeFieldsCSV = includeFields.joinToString(", ")
          val missingFieldsCSV = missingFields.joinToString(", ")
          val errorMessage =
            "The includeFields specified, $includeFieldsCSV, does not include the " +
              "following primary-key fields: $missingFieldsCSV."
          return SyncResult.Error.InvalidArgument(
            srcSchema = srcSchema,
            srcTable = srcTable,
            destSchema = destSchema,
            destTable = destTable,
            errorMessage = errorMessage,
            originalError = null,
            argumentName = "includeFields",
            argumentValue = includeFields,
          )
        }
      }

    val fieldNamesFinal = fieldsFinal.map { fld -> fld.name }.toSet()

    val copyTableResult =
      copyTable(
        srcCon = srcCon,
        destCon = destCon,
        srcDialect = srcDialect,
        destDialect = destDialect,
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        includeFields = fieldNamesFinal,
        primaryKeyFields = pkFieldsFinal,
      )

    val destTableDef: Table =
      when (copyTableResult) {
        is CopyTableResult.Error ->
          return SyncResult.Error.CopyTableFailed(
            srcSchema = srcSchema,
            srcTable = srcTable,
            destSchema = destSchema,
            destTable = destTable,
            errorMessage = "If a value is provided, then it must contain at least one field name.",
            originalError = copyTableResult.originalError,
            srcTableDef = actualSrcTableDef,
          )
        is CopyTableResult.Success -> copyTableResult.destTableDef
      }

    val srcTableDef: Table =
      actualSrcTableDef.copy(
        fields = fieldsFinal,
        primaryKeyFieldNames = pkFieldsFinal,
      )

    val compareFieldNamesFinal: Set<String> =
      if (compareFields == null) {
        fieldNamesFinal.minus(srcTableDef.primaryKeyFieldNames)
      } else {
        fieldNamesFinal.filter { fldName -> fldName in compareFields }.toSet()
      }

    val lkpTableFieldNames = pkFieldsFinal.union(compareFieldNamesFinal)
    val lkpTableFields = srcTableDef.fields.filter { fld -> fld.name in lkpTableFieldNames }.toSet()

    val srcLkpTable = srcTableDef.subset(fieldNames = lkpTableFieldNames)
    val srcKeysSQL: String = src.adapter.select(table = srcLkpTable, criteria = criteria)
    val srcLkpRows: Set<Row> = src.executor.fetchRows(sql = srcKeysSQL, fields = lkpTableFields)

    val destLkpTable = destTableDef.subset(fieldNames = lkpTableFieldNames)
    val destKeysSQL: String = dest.adapter.select(table = destLkpTable, criteria = criteria)
    val destLkpRows = dest.executor.fetchRows(sql = destKeysSQL, fields = lkpTableFields)

    val rowDiff: RowDiff =
      try {
        compareRows(
          old = destLkpRows,
          new = srcLkpRows,
          primaryKeyFields = pkFieldsFinal.toSet(),
          compareFields = compareFieldNamesFinal,
          includeFields = lkpTableFieldNames,
        )
      } catch (e: Exception) {
        return SyncResult.Error.RowComparisonFailed(
          srcSchema = srcSchema,
          srcTable = srcTable,
          destSchema = destSchema,
          destTable = destTable,
          errorMessage = "An error occurred while executing compareRows: $e",
          originalError = null,
          srcRows = srcLkpRows,
          destRows = destLkpRows,
          pkFields = pkFieldsFinal.toSet(),
          compareFields = compareFieldNamesFinal,
          includeFields = lkpTableFieldNames,
        )
      }

    try {
      if (rowDiff.added.keys.isNotEmpty()) {
        val selectSQL: String =
          src.adapter.selectKeys(table = srcTableDef, primaryKeyValues = rowDiff.added.keys)
        val addedRows: Set<Row> = src.executor.fetchRows(sql = selectSQL, fields = srcTableDef.fields)
        val insertSQL: String = dest.adapter.add(table = destTableDef, rows = addedRows)
        dest.executor.execute(sql = insertSQL)
      }
    } catch (e: Exception) {
      return SyncResult.Error.AddRowsFailed(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "An error occurred while adding rows: $e",
        originalError = e,
        rows = rowDiff.added.values.toSet(),
      )
    }

    try {
      if (rowDiff.deleted.keys.isNotEmpty()) {
        val deleteSQL: String =
          dest.adapter.deleteKeys(table = destTableDef, primaryKeyValues = rowDiff.deleted.keys)
        dest.executor.execute(sql = deleteSQL)
      }
    } catch (e: Exception) {
      return SyncResult.Error.DeleteRowsFailed(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "An error occurred while deleting rows: $e",
        originalError = e,
        rows = rowDiff.deleted.values.toSet(),
      )
    }

    try {
      if (rowDiff.updated.isNotEmpty()) {
        val selectSQL: String =
          src.adapter.selectKeys(table = srcTableDef, primaryKeyValues = rowDiff.updated.keys)
        val fullRows: Set<Row> = src.executor.fetchRows(sql = selectSQL, fields = fieldsFinal)
        val updateSQL: String = dest.adapter.update(table = destTableDef, rows = fullRows)
        dest.executor.execute(sql = updateSQL)
      }
    } catch (e: Exception) {
      return SyncResult.Error.UpdateRowsFailed(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "An error occurred while updating rows: $e",
        originalError = e,
        rows = rowDiff.updated.values.toSet(),
      )
    }

    return SyncResult.Success(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      added = rowDiff.added.count(),
      deleted = rowDiff.deleted.count(),
      updated = rowDiff.updated.count(),
    )
  } catch (e: Exception) {
    return SyncResult.Error.Unexpected(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      errorMessage = "An unexpected error occurred while syncing: ${e.message}",
      originalError = e,
    )
  }
}
