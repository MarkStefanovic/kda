package kda.adapter.mssql

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import kda.domain.Criteria
import kda.domain.DbDialect
import kda.domain.Field
import kda.domain.OrderBy
import kda.domain.Row
import kda.domain.Table
import java.sql.Connection

@ExperimentalStdlibApi
class MSSQLAdapter(con: Connection, showSQL: Boolean) : Adapter {
  private val stdAdapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = MSSQLAdapterDetails,
    dialect = DbDialect.MSSQL,
  )

  override fun createTable(schema: String?, table: Table) =
    stdAdapter.createTable(schema = schema, table = table)

  override fun delete(
    schema: String?,
    table: String,
    criteria: Criteria,
  ) =
    stdAdapter.delete(
      schema = schema,
      table = table,
      criteria = criteria,
    )

  override fun deleteAll(schema: String?, table: String) =
    stdAdapter.deleteAll(schema = schema, table = table)

  override fun deleteRows(schema: String?, table: String, fields: Set<Field<*>>, keys: Set<Row>) =
    stdAdapter.deleteRows(schema = schema, table = table, fields = fields, keys = keys)

  override fun select(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    criteria: Criteria?,
    batchSize: Int,
    limit: Int?,
    orderBy: List<OrderBy>,
  ): Sequence<Row> =
    stdAdapter.select(
      schema = schema,
      table = table,
      fields = fields,
      criteria = criteria,
      batchSize = batchSize,
      orderBy = orderBy,
      limit = limit,
    )

  override fun selectAll(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    batchSize: Int,
    orderBy: List<OrderBy>,
  ): Sequence<Row> =
    stdAdapter.selectAll(
      schema = schema,
      table = table,
      fields = fields,
      batchSize = batchSize,
      orderBy = orderBy,
    )

  override fun selectRows(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    keys: Set<Row>,
    batchSize: Int,
    orderBy: List<OrderBy>,
  ): Sequence<Row> =
    stdAdapter.selectRows(
      schema = schema,
      table = table,
      fields = fields,
      keys = keys,
      batchSize = batchSize,
      orderBy = orderBy,
    )

  override fun upsertRows(
    schema: String?,
    table: String,
    rows: Set<Row>,
    keyFields: Set<Field<*>>,
    valueFields: Set<Field<*>>,
  ) {
    stdAdapter.upsertRows(
      schema = schema,
      table = table,
      rows = rows,
      keyFields = keyFields,
      valueFields = valueFields,
    )
  }
}
