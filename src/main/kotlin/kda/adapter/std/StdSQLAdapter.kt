package kda.adapter.std

import kda.domain.*

open class StdSQLAdapter(private val impl: SQLAdapterImplDetails) : SQLAdapter {
  override fun add(table: Table, rows: Set<Row>): String {
    val fieldNames = table.sortedFieldNames
    val fieldNameCSV = fieldNames.joinToString(", ") { fldName -> impl.wrapName(fldName) }
    val valuesCSV = impl.valuesExpression(fields = table.fields, rows = rows)
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    return "INSERT INTO $tableName ($fieldNameCSV) VALUES $valuesCSV"
  }

  override fun createTable(table: Table): String {
    val colDefCSV = table.fields.joinToString(", ") { fld -> impl.fieldDef(fld) }
    val pkCSV =
      table.primaryKeyFieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    return "CREATE TABLE $tableName ($colDefCSV, PRIMARY KEY ($pkCSV))"
  }

  override fun deleteKeys(table: Table, primaryKeyValues: Set<Row>): String {
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)

    return if (table.primaryKeyFieldNames.count() > 1) {
      val whereClause =
        table.primaryKeyFieldNames //
          .map { fld -> impl.wrapName(fld) }
          .joinToString(" AND ") { fld -> "t.$fld = d.$fld" }
      val valuesCTE = impl.valuesCTE(
        cteName = "d",
        fields = table.primaryKeyFields.toSet(),
        rows = primaryKeyValues,
      )
      "WITH $valuesCTE DELETE FROM $tableName t USING d WHERE $whereClause"
    } else {
      val pk = table.primaryKeyFields.first()
      val pkName = impl.wrapName(pk.name)
      val valuesCSV = primaryKeyValues.joinToString(", ") { row ->
        impl.wrapValue(value = row.value(pk.name), dataType = pk.dataType)
      }
      "DELETE FROM $tableName WHERE $pkName IN ($valuesCSV)"
    }
  }

  override fun dropTable(schema: String?, table: String): String =
    "DROP TABLE ${impl.fullTableName(schema = schema, table = table)}"

  override fun getRowCount(schema: String?, table: String): String =
    "SELECT COUNT(*) AS ${impl.wrapName("rows")} FROM ${impl.fullTableName(schema = schema, table = table)}"

  override fun merge(table: Table, rows: Set<Row>): String {
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    val insertClause = impl.fieldNameCSV(fieldNames = table.sortedFieldNames.toSet(), tableAlias = "v")
    val joinOnFields = impl.joinFields(
      table = table,
      leftTableAlias = "t",
      rightTableAlias = "v",
    )
    val setValuesCSV = impl.setValues(
      table = table,
      rightTableAlias = "v",
    )
    val valuesCTE = impl.valuesCTE(
      cteName = "v",
      fields = table.fields,
      rows = rows,
    )
    return "WITH $valuesCTE " +
      "MERGE INTO $tableName t " +
      "USING v ON $joinOnFields " +
      "WHEN NOT MATCHED " +
      "INSERT VALUES ($insertClause) " +
      "WHEN MATCHED " +
      "UPDATE SET $setValuesCSV"
  }

  override fun select(table: Table, criteria: Set<Criteria>): String {
    val selectClause = impl.fieldNameCSV(table.sortedFieldNames.toSet())
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    val selectSQL = "SELECT $selectClause FROM $tableName"
    return if (criteria.isEmpty()) {
      selectSQL
    } else {
      val whereClause = impl.renderCriteria(criteria)
      if (whereClause == null) {
        selectSQL
      } else {
        "$selectSQL WHERE $whereClause"
      }
    }
  }

  override fun selectKeys(table: Table, primaryKeyValues: Set<Row>): String {
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    val pkCols = table.primaryKeyFieldNames
    return if (pkCols.count() > 1) {
      val colNameCSV = table.sortedFieldNames.joinToString(", ") { fld -> "t." + impl.wrapName(fld) }
      val joinClause =
        pkCols
          .map { fld -> impl.wrapName(fld) }
          .joinToString(" AND ") { fld -> "t.$fld = v.$fld" }
      val valuesCTE = impl.valuesCTE(
        cteName = "v",
        fields = table.primaryKeyFields.toSet(),
        rows = primaryKeyValues,
      )
      "WITH $valuesCTE " +
        "SELECT $colNameCSV FROM $tableName t " +
        "JOIN v ON $joinClause"
    } else {
      val colNameCSV = table.sortedFieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
      val pk = table.primaryKeyFields.first()
      val pkName = impl.wrapName(pkCols.first())
      val valuesCSV =
        primaryKeyValues.joinToString(", ") { row ->
          impl.wrapValue(value = row.value(pk.name), dataType = pk.dataType)
        }
      "SELECT $colNameCSV FROM $tableName WHERE $pkName IN ($valuesCSV)"
    }
  }

  override fun selectMaxValues(table: Table, fieldNames: Set<String>): String {
    val selectCSV = fieldNames.joinToString(", ") { fld -> impl.maxValue(fld) }
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    return "SELECT $selectCSV FROM $tableName"
  }

  override fun update(table: Table, rows: Set<Row>): String {
    val whereClause = impl.joinFields(table = table, leftTableAlias = "t", rightTableAlias = "u")
    val setClause = impl.setValues(table = table, rightTableAlias = "u")
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)
    val valuesCTE = impl.valuesCTE(
      cteName = "u",
      fields = table.fields,
      rows = rows,
    )
    return "WITH $valuesCTE " +
      "UPDATE $tableName AS t SET $setClause FROM u WHERE $whereClause"
  }
}
