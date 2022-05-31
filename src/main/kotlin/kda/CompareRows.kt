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
  srcCriteria: Criteria? = null,
  dstCriteria: Criteria? = null,
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

  val srcAdapter: Adapter = selectAdapter(
    dialect = srcDialect,
    con = srcCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  )

  val dstAdapter: Adapter = selectAdapter(
    dialect = dstDialect,
    con = dstCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  )

  val tables: CopyTableResult = copyTable(
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
    showSQL = showSQL,
  )

  val dstTs = if (timestampFieldNames.isEmpty()) {
    null
  } else {
    getLatestTs(
      adapter = dstAdapter,
      schema = dstSchema,
      table = tables.dstTable,
      tsFieldNames = timestampFieldNames,
    )
  }

  val srcFullCriteria = getFullCriteria(
    dialect = srcDialect,
    table = tables.srcTable,
    tsFieldNames = timestampFieldNames,
    criteria = srcCriteria,
    ts = dstTs,
  )

  val dstFullCriteria = getFullCriteria(
    dialect = dstDialect,
    table = tables.dstTable,
    tsFieldNames = timestampFieldNames,
    criteria = dstCriteria,
    ts = dstTs,
  )

  val srcRows: Set<Row> = fetchLookupTable(
    adapter = srcAdapter,
    primaryKeyFieldNames = primaryKeyFieldNames,
    compareFields = compareFields,
    criteria = srcFullCriteria,
    schema = srcSchema,
    table = tables.srcTable,
    batchSize = batchSize,
  )

  val dstRows: Set<Row> = fetchLookupTable(
    adapter = dstAdapter,
    primaryKeyFieldNames = primaryKeyFieldNames,
    compareFields = compareFields,
    criteria = dstFullCriteria,
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
private fun getLatestTs(
  adapter: Adapter,
  schema: String?,
  table: Table,
  tsFieldNames: Set<String>,
): Any? {
  val tsFields: Set<Field<*>> =
    tsFieldNames
      .map { fieldName ->
        table.field(fieldName)
      }
      .toSet()

  return adapter.selectGreatest(
    schema = schema,
    table = table.name,
    fields = tsFields,
  )
}

@ExperimentalStdlibApi
private fun getFullCriteria(
  dialect: DbDialect,
  table: Table,
  tsFieldNames: Set<String>,
  criteria: Criteria?,
  ts: Any?,
): Criteria? =
  if (tsFieldNames.isEmpty()) {
    criteria
  } else {
    val tsFields: Set<Field<*>> =
      tsFieldNames
        .map { fieldName ->
          table.field(fieldName)
        }
        .toSet()

    val tsCriteria: Criteria? = if (ts == null) {
      null
    } else {
      var c = where(dialect)
      tsFields.forEach { field ->
        c = c.or(
          BinaryPredicate(
            parameterName = field.name,
            dataType = field.dataType,
            operator = Operator.GreaterThan,
            value = ts,
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
