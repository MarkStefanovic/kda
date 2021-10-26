package kda.adapter.pg

import kda.adapter.std.StdSQLAdapter
import kda.domain.Row
import kda.domain.SQLAdapterImplDetails
import kda.domain.Table

class PgSQLAdapter(private val impl: SQLAdapterImplDetails) : StdSQLAdapter(impl) {
  override fun merge(table: Table, rows: Set<Row>): String {
    val valuesCSV = impl.valuesExpression(
      fieldNames = table.sortedFieldNames.toSet(),
      rows = rows,
      tableAlias = null,
    )
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    val colNameCSV = impl.fieldNameCSV(table.sortedFieldNames.toSet())
    val pkCSV = impl.fieldNameCSV(table.primaryKeyFieldNames.toSet())
    val setValuesCSV = impl.setValues(table = table, rightTableAlias = "EXCLUDED")
    return "INSERT INTO $tableName ($colNameCSV) " +
      "VALUES $valuesCSV " +
      "ON CONFLICT ($pkCSV) " +
      "DO UPDATE SET $setValuesCSV"
  }
}

val pgSQLAdapter by lazy { PgSQLAdapter(PgSQLAdapterImplDetails()) }
