package kda

import kda.adapter.selectAdapter
import kda.domain.Cache
import kda.domain.CopyTableResult
import kda.domain.DbDialect
import kda.domain.KDAError
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun copyTable(
  srcCon: Connection,
  dstCon: Connection,
  dstDialect: DbDialect,
  srcDbName: String,
  srcSchema: String?,
  srcTable: String,
  dstDbName: String,
  dstSchema: String?,
  dstTable: String,
  primaryKeyFieldNames: List<String>,
  cache: Cache,
  showSQL: Boolean = false,
  includeFields: Set<String>? = null,
  queryTimeout: Duration = 30.minutes,
): CopyTableResult {
  val srcTableDef = inspectTable(
    con = srcCon,
    cache = cache,
    dbName = srcDbName,
    schema = srcSchema,
    table = srcTable,
    primaryKeyFieldNames = primaryKeyFieldNames,
    includeFieldNames = includeFields,
  )

  if (
    !cache.tableExists(
      con = srcCon,
      dbName = srcDbName,
      schema = srcSchema,
      table = srcTable,
    )
  ) {
    throw KDAError.TableNotFound(schema = srcSchema, table = srcTable)
  }

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

  val dstAdapter = selectAdapter(
    dialect = dstDialect,
    con = dstCon,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
  )

  if (
    !cache.tableExists(
      con = dstCon,
      dbName = dstDbName,
      schema = dstSchema,
      table = dstTable,
    )
  ) {
    dstAdapter.createTable(schema = dstSchema, table = dstTableDef)
  }

  return CopyTableResult(
    srcTable = srcTableDef,
    dstTable = dstTableDef,
  )
}
