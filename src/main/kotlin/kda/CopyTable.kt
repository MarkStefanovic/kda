package kda

import kda.adapter.selectAdapter
import kda.adapter.tableExists
import kda.domain.Cache
import kda.domain.CopyTableResult
import kda.domain.DbDialect
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun copyTable(
  srcCon: Connection,
  dstCon: Connection,
  dstDialect: DbDialect,
  srcSchema: String?,
  srcTable: String,
  dstSchema: String?,
  dstTable: String,
  primaryKeyFieldNames: List<String>,
  cache: Cache,
  showSQL: Boolean = false,
  includeFields: Set<String>? = null,
  queryTimeout: Duration = Duration.minutes(30),
): CopyTableResult {

  val srcTableDef = inspectTable(
    con = srcCon,
    cache = cache,
    schema = srcSchema,
    table = srcTable,
    primaryKeyFieldNames = primaryKeyFieldNames,
    includeFieldNames = includeFields,
  )

  val includeFieldDefs = if (includeFields == null) {
    srcTableDef.fields
  } else {
    srcTableDef.fields
      .filter { fld -> fld.name in includeFields }
      .toSet()
  }

  val dstTableDef = srcTableDef.copy(
    name = dstTable,
    fields = includeFieldDefs,
    primaryKeyFieldNames = primaryKeyFieldNames,
  )

  val dstAdapter = selectAdapter(dialect = dstDialect, con = dstCon, showSQL = showSQL, queryTimeout = queryTimeout)

  if (!tableExists(con = dstCon, schema = dstSchema, table = dstTable)) {
    dstAdapter.createTable(schema = dstSchema, table = dstTableDef)
  }

  return CopyTableResult(
    srcTable = srcTableDef,
    dstTable = dstTableDef,
  )
}
