package kda.adapter.mssql

import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.Row

class MSSQLAdapterImplDetails : StdSQLAdapterImplDetails() {
  override fun wrapName(name: String) =
    "[${name.lowercase().replace("[", "").replace("]", "")}]"

  override fun valuesCTE(cteName: String, fieldNames: Set<String>, rows: Set<Row>): String {
    val sortedFieldNames = fieldNames.sorted()
    val unions = rows.joinToString(" UNION ALL ") { row ->
      "SELECT " + sortedFieldNames.joinToString(", ") { fieldName ->
        val wrappedName = wrapName(fieldName)
        val wrappedValue = wrapValue(row.value(fieldName))
        "$wrappedValue AS $wrappedName"
      }
    }
    return "$cteName AS ($unions)"
  }
}
