package kda

import kda.adapter.selectAdapter
import kda.adapter.where
import kda.domain.Adapter
import kda.domain.BinaryPredicate
import kda.domain.Cache
import kda.domain.CopyTableResult
import kda.domain.Criteria
import kda.domain.DbDialect
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Operator
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.Table
import java.sql.Connection
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun compareRows(
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
  compareFields: Set<String>,
  primaryKeyFieldNames: List<String>,
  cache: Cache,
  criteria: Criteria? = null,
  includeFields: Set<String>? = null,
  timestampFieldNames: Set<String> = setOf(),
  showSQL: Boolean = false,
  batchSize: Int = 1_000,
  queryTimeout: Duration = 30.minutes,
  timestampResolution: ChronoUnit = ChronoUnit.MILLIS,
): RowDiff {
  if (compareFields.isEmpty()) {
    throw KDAError.InvalidArgument(
      errorMessage = "If a value is provided, then it must contain at least one field name.",
      argumentName = "compareFields",
      argumentValue = compareFields,
    )
  }

  val srcAdapter: Adapter =
    selectAdapter(
      dialect = srcDialect,
      con = srcCon,
      showSQL = showSQL,
      queryTimeout = queryTimeout,
      timestampResolution = timestampResolution,
    )

  val dstAdapter: Adapter =
    selectAdapter(
      dialect = dstDialect,
      con = dstCon,
      showSQL = showSQL,
      queryTimeout = queryTimeout,
      timestampResolution = timestampResolution,
    )

  val tables: CopyTableResult =
    copyTable(
      srcCon = srcCon,
      dstCon = dstCon,
      cache = cache,
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

  val fullCriteria: Criteria? =
    getFullCriteria(
      dstAdapter = dstAdapter,
      dstDialect = dstDialect,
      dstSchema = dstSchema,
      dstTable = tables.dstTable,
      tsFieldNames = timestampFieldNames,
      criteria = criteria,
    )

  val srcRows: Set<Row> =
    fetchLookupTable(
      adapter = srcAdapter,
      primaryKeyFieldNames = primaryKeyFieldNames,
      compareFields = compareFields,
      criteria = fullCriteria,
      schema = srcSchema,
      table = tables.srcTable,
      batchSize = batchSize,
    )

  val dstRows: Set<Row> =
    fetchLookupTable(
      adapter = dstAdapter,
      primaryKeyFieldNames = primaryKeyFieldNames,
      compareFields = compareFields,
      criteria = fullCriteria,
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

    val latestTimestamp: LocalDateTime? =
      dstAdapter.selectGreatest(
        schema = dstSchema,
        table = dstTable.name,
        fields = tsFields,
      ) as LocalDateTime?

    val tsCriteria: Criteria? = if (latestTimestamp == null) {
      null
    } else {
      var c = where(dstDialect)
      tsFields.forEach { field ->
        c = c.or(
          BinaryPredicate(
            parameterName = field.name,
            dataType = field.dataType,
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
