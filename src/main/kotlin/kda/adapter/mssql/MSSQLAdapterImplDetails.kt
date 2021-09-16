package kda.adapter.mssql

import kda.adapter.std.StdSQLAdapterImplDetails

class MSSQLAdapterImplDetails : StdSQLAdapterImplDetails() {
  override fun wrapName(name: String) =
    "[${name.lowercase()}]"
}
