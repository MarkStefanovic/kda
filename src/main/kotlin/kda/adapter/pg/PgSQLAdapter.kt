package kda.adapter.pg

import kda.adapter.std.StdSQLAdapter
import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.Row
import kda.domain.SQLAdapter
import kda.domain.Table

class PgSQLAdapter(private val std: SQLAdapter, private val implDetails: PgSQLAdapterImplDetails) : SQLAdapter by std {
  override fun merge(table: Table, rows: Set<Row>): String {
    val valuesCSV = implDetails.valuesExpression(fieldNames = table.sortedFieldNames, rows = rows, tableAlias = null)
    val tableName = implDetails.fullTableName(schema = table.schema, table = table.name)
    val colNameCSV = implDetails.fieldNameCSV(table.sortedFieldNames.toSet())
    val pkCSV = implDetails.fieldNameCSV(table.primaryKeyFieldNames.toSet())
    val setValuesCSV = implDetails.setValues(table = table, rightTableAlias = "EXCLUDED")
    return "INSERT INTO $tableName ($colNameCSV) " +
      "VALUES $valuesCSV " +
      "ON CONFLICT ($pkCSV) " +
      "DO UPDATE SET $setValuesCSV"
  }
}

private val implDetails = PgSQLAdapterImplDetails(StdSQLAdapterImplDetails())

val pgSQLAdapter =
  PgSQLAdapter(StdSQLAdapter(implDetails), implDetails = implDetails)
