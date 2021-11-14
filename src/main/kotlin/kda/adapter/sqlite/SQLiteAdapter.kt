package kda.adapter.sqlite

import kda.adapter.std.StdSQLAdapter
import kda.domain.Row
import kda.domain.SQLAdapterImplDetails
import kda.domain.Table

class SQLiteAdapter(private val impl: SQLAdapterImplDetails) : StdSQLAdapter(impl) {
  override fun delete(table: Table, rows: Set<Row>): String {
    val tableName = impl.fullTableName(schema = table.schema, table = table.name)

    return if (rows.isEmpty()) {
      ""
    } else {
      val sortedFields =
        rows
          .first()
          .fieldNames
          .sorted()
          .map { table.field(it) }

      if (table.primaryKeyFieldNames.count() > 1) {
        val whereClause =
          sortedFields
            .joinToString(" AND ") { fld ->
              val fieldName = impl.wrapName(fld.name)

              if (fld.nullable) {
                "($tableName.$fieldName = d.$fieldName OR ($tableName.$fieldName IS NULL AND d.$fieldName IS NULL))"
              } else {
                "$tableName.$fieldName = d.$fieldName"
              }
            }

        val valuesCTE = impl.valuesCTE(
          cteName = "d",
          fields = sortedFields.toSet(),
          rows = rows,
        )

        "WITH $valuesCTE DELETE FROM $tableName WHERE EXISTS (SELECT 1 FROM d WHERE $whereClause)"
      } else {
        val pk = sortedFields.first()

        val pkName = impl.wrapName(pk.name)

        val valuesCSV = rows.joinToString(", ") { row ->
          impl.wrapValue(value = row.value(pk.name), dataType = pk.dataType)
        }

        "DELETE FROM $tableName WHERE $pkName IN ($valuesCSV)"
      }
    }
  }

  override fun merge(table: Table, rows: Set<Row>): String {
    val valuesCSV = impl.valuesExpression(
      fields = table.fields,
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

val sqliteAdapter by lazy { SQLiteAdapter(SQLiteAdapterImplDetails()) }
