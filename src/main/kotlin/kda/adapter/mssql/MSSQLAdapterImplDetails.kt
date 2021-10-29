package kda.adapter.mssql

import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.Field
import kda.domain.Row

class MSSQLAdapterImplDetails : StdSQLAdapterImplDetails() {
  override fun wrapName(name: String) =
    "[${name.lowercase().replace("[", "").replace("]", "")}]"

  override fun valuesCTE(cteName: String, fields: Set<Field>, rows: Set<Row>): String {
    val sortedFields = fields.sortedBy { it.name }
    val unions = rows.joinToString(" UNION ALL ") { row ->
      "SELECT " + sortedFields.joinToString(", ") { fld ->
        val wrappedName = wrapName(fld.name)
        val wrappedValue = wrapValue(value = row.value(fld.name), dataType = fld.dataType)
        "$wrappedValue AS $wrappedName"
      }
    }
    return "$cteName AS ($unions)"
  }
}
