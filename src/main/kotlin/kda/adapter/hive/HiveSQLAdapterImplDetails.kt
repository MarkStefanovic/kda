package kda.adapter.hive

import kda.adapter.std.StdSQLAdapterImplDetails

class HiveSQLAdapterImplDetails : StdSQLAdapterImplDetails() {
  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "NULL"
      value -> "TRUE"
      else -> "FALSE"
    }

  override fun wrapName(name: String) =
    "`${name.lowercase()}`"
}
