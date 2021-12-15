package kda

import kda.adapter.selectAdapter
import kda.adapter.where
import kda.domain.Adapter
import kda.domain.BinaryPredicate
import kda.domain.CopyTableResult
import kda.domain.Criteria
import kda.domain.DataType
import kda.domain.DbDialect
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Operator
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.SyncResult
import kda.domain.Table
import kda.domain.compareRows
import java.sql.Connection
import java.sql.Timestamp
import java.time.LocalDateTime

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

  val lkpTableFieldNames = pkFields.union(compareFieldNamesFinal)

  val lkpTableFields =
    tables.srcTable.fields.filter { fld -> fld.name in lkpTableFieldNames }.toSet()

  val fullCriteria = getFullCriteria(
    dstAdapter = dstAdapter,
    dstDialect = dstDialect,
    dstSchema = dstSchema,
    dstTable = tables.dstTable,
    tsFieldNames = timestampFieldNames,
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

@ExperimentalStdlibApi
private fun getFullCriteria(
  dstAdapter: Adapter,
  dstDialect: DbDialect,
  dstSchema: String?,
  dstTable: Table,
  tsFieldNames: Set<String>,
  criteria: Criteria?,
): Criteria? =
  if (tsFieldNames.isEmpty()) {
    criteria
  } else {
    val tsFields: Set<Field<*>> =
      tsFieldNames
        .map { fieldName ->
          dstTable.field(fieldName)
        }
        .toSet()

    val latestTimestamp: LocalDateTime? = (
      dstAdapter.selectGreatest(
        schema = dstSchema,
        table = dstTable.name,
        fields = tsFields,
      ) as Timestamp?
      )?.toLocalDateTime()

    val tsCriteria: Criteria? = if (latestTimestamp == null) {
      null
    } else {
      var c = where(dstDialect)
      tsFields.forEach { field ->
        c = c.or(
          BinaryPredicate(
            parameterName = field.name,
            dataType = DataType.localDateTime,
            operator = Operator.GreaterThan,
            value = latestTimestamp,
          )
        )
      }
      c
    }

    if (tsCriteria == null) {
      criteria
    } else {
      criteria?.and(tsCriteria) ?: tsCriteria
    }
  }
