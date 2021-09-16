package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.mssql.mssqlDatasource
import kda.adapter.pg.pgDatasource
import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.Dialect
import kda.domain.InspectTableResult
import kda.domain.Row
import kda.domain.RowDiff
import kda.domain.flatMap
import java.sql.Connection

fun compareRows(
  srcCon: Connection,
  destCon: Connection,
  srcDialect: Dialect,
  destDialect: Dialect,
  srcSchema: String?,
  srcTable: String,
  destSchema: String?,
  destTable: String,
  compareFields: Set<String>,
  primaryKeyFieldNames: List<String>,
  criteria: Set<Criteria> = emptySet(),
  cache: Cache = DbCache(),
): Result<RowDiff> = runCatching {
  val src: Datasource = when (srcDialect) {
    Dialect.HortonworksHive -> hiveDatasource(con = srcCon)
    Dialect.MSSQLServer -> mssqlDatasource(con = srcCon)
    Dialect.PostgreSQL -> pgDatasource(con = srcCon)
  }

  val dest: Datasource = when (destDialect) {
    Dialect.HortonworksHive -> hiveDatasource(con = destCon)
    Dialect.MSSQLServer -> mssqlDatasource(con = destCon)
    Dialect.PostgreSQL -> pgDatasource(con = destCon)
  }

  val includeFieldNames = primaryKeyFieldNames.toSet().union(compareFields)

  return fetchLookupTable(
    ds = src,
    con = srcCon,
    dialect = srcDialect,
    schema = srcSchema,
    table = srcTable,
    primaryKeyFieldNames = primaryKeyFieldNames,
    compareFields = compareFields,
    criteria = criteria,
    cache = cache,
  )
    .flatMap { srcRows ->
      fetchLookupTable(
        ds = dest,
        con = destCon,
        dialect = destDialect,
        schema = destSchema,
        table = destTable,
        primaryKeyFieldNames = primaryKeyFieldNames,
        compareFields = compareFields,
        criteria = criteria,
        cache = cache,
      )
        .flatMap { destRows ->
          runCatching {
            kda.domain.compareRows(
              dest = destRows,
              src = srcRows,
              primaryKeyFields = primaryKeyFieldNames.toSet(),
              compareFields = compareFields,
              includeFields = includeFieldNames,
            )
          }
        }
    }
}

fun fetchLookupTable(
  ds: Datasource,
  con: Connection,
  dialect: Dialect,
  schema: String?,
  table: String,
  primaryKeyFieldNames: List<String>,
  compareFields: Set<String>,
  criteria: Set<Criteria>,
  cache: Cache,
): Result<Set<Row>> = runCatching {
  val includeFieldNames = primaryKeyFieldNames.toSet().union(compareFields)
  val tableDefResult = inspectTable(
    con = con,
    dialect = dialect,
    schema = schema,
    table = table,
    primaryKeyFieldNames = primaryKeyFieldNames,
    includeFieldNames = includeFieldNames,
    cache = cache,
  )
  return when (tableDefResult) {
    is InspectTableResult.Error -> Result.failure(
      tableDefResult.originalError
        ?: Exception("An error occurred while inspecting $schema.$table.")
    )
    is InspectTableResult.Success -> {
      val srcLkpTable = tableDefResult.tableDef.subset(includeFieldNames)
      val includeFields = tableDefResult.tableDef.fields.filter { it.name in includeFieldNames }.toSet()
      val srcKeysSQL: String = ds.adapter.select(table = srcLkpTable, criteria = criteria)
      val rows = ds.executor.fetchRows(sql = srcKeysSQL, fields = includeFields)
      Result.success(rows)
    }
  }
}
