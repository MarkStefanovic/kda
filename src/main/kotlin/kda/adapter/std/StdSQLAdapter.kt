package kda.adapter.std

import kda.domain.*

class StdSQLAdapter(private val impl: SQLAdapterImplDetails) : SQLAdapter {
    override fun createTable(table: Table): String {
        val colDefCSV = table.fields.joinToString(", ") { fld -> impl.fieldDef(fld) }
        val pkCSV =
            table.primaryKeyFieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
        val tableName = fullTableName(schema = table.schema, table = table.name)
        return "CREATE TABLE $tableName ($colDefCSV, PRIMARY KEY ($pkCSV))"
    }

    override fun dropTable(schema: String?, table: String): String {
        return "DROP TABLE ${fullTableName(schema = schema, table = table)}"
    }

  override fun add(table: Table, rows: Set<Row>): String {
    val fieldNames = table.sortedFieldNames
    val fieldNameCSV = fieldNames.joinToString(", ") { fldName -> impl.wrapName(fldName) }
    val valuesCSV = impl.valuesExpression(fieldNames = fieldNames, rows = rows)
    val tableName = fullTableName(schema = table.schema, table = table.name)
    return "INSERT INTO $tableName ($fieldNameCSV) VALUES $valuesCSV"
  }

  override fun delete(table: Table, primaryKeyValues: Set<Row>): String {
    val tableName = fullTableName(schema = table.schema, table = table.name)

    return if (table.primaryKeyFieldNames.count() > 1) {
      val whereClause =
        table.primaryKeyFieldNames //
          .map { fld -> impl.wrapName(fld) }
          .joinToString(" AND ") { fld -> "t.$fld = d.$fld" }
      val pkColNames = pkColNameCSV(table)
      val valuesCSV =
        primaryKeyValues.joinToString(", ") { row ->
          val pkValCSV =
            table.primaryKeyFieldNames.joinToString(", ") { fld ->
              impl.wrapValue(row.value(fld))
            }
          "($pkValCSV)"
        }
      "WITH d ($pkColNames) AS (VALUES $valuesCSV) " +
        "DELETE FROM $tableName t USING d WHERE $whereClause"
    } else {
      val pkCol = impl.wrapName(table.primaryKeyFieldNames.first())
      val valuesCSV =
        primaryKeyValues
          .flatMap { row ->
            table.primaryKeyFieldNames.map { fld ->
              impl.wrapValue(row.value(fld))
            }
          }
          .joinToString(", ")
      "DELETE FROM $tableName WHERE $pkCol IN ($valuesCSV)"
    }
  }

  override fun update(table: Table, rows: Set<Row>): String {
    val fieldNames = table.sortedFieldNames
    val colNameCSV = fieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
    val whereClause =
      table.primaryKeyFieldNames //
        .map { fld -> impl.wrapName(fld) }
        .joinToString("AND ") { fld -> "t.$fld = u.$fld" }
    val nonPKCols = fieldNames.toSet() - table.primaryKeyFieldNames.toSet()
    val setClause =
      nonPKCols //
        .sorted()
        .joinToString(", ") { fld -> //
          "t.${impl.wrapName(fld)} = u.${impl.wrapName(fld)}"
        }
    val tableName = fullTableName(schema = table.schema, table = table.name)
    val valuesCSV = impl.valuesExpression(fieldNames = fieldNames, rows = rows)
    return "WITH u ($colNameCSV) AS (VALUES $valuesCSV) " +
      "UPDATE $tableName AS t SET $setClause FROM u ON $whereClause"
  }

  override fun select(table: Table): String {
    val colNameCSV = table.sortedFieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
    val tableName = fullTableName(schema = table.schema, table = table.name)
    return "SELECT $colNameCSV FROM $tableName"
  }

  override fun selectKeys(table: Table, primaryKeyValues: Set<Row>): String {
    val colNameCSV = table.sortedFieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
    val tableName = fullTableName(schema = table.schema, table = table.name)
    val pkCols = table.primaryKeyFieldNames
    return if (pkCols.count() > 1) {
      val pkColNameCSV = pkCols.joinToString(", ") { fld -> impl.wrapName(fld) }
      val pkValues = impl.valuesExpression(fieldNames = pkCols, rows = primaryKeyValues)
      val whereClause =
        pkCols //
          .map { fld -> impl.wrapName(fld) }
          .joinToString(" AND ") { fld -> "t.$fld = d.$fld" }
      "WITH v ($pkColNameCSV) AS (VALUES $pkValues) " +
        "SELECT $colNameCSV FROM $tableName t " +
        "JOIN v ON $whereClause"
    } else {
      val pkCol = impl.wrapName(pkCols.first())
      val valuesCSV =
        primaryKeyValues.joinToString(", ") { row ->
          impl.wrapValue(row.value(pkCol))
        }
      "SELECT $colNameCSV FROM $tableName WHERE $pkCol IN ($valuesCSV)"
    }
  }

  private fun fullTableName(schema: String?, table: String): String =
    if (schema == null) impl.wrapName(table)
    else "${impl.wrapName(schema)}.${impl.wrapName(table)}"

  private fun pkColNameCSV(table: Table): String =
    table.primaryKeyFieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
}
