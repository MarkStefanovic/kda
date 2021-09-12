package kda.adapter.pg

import kda.adapter.std.StdSQLAdapterImplDetails

class PgSQLAdapterImplDetails : StdSQLAdapterImplDetails() {
  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "NULL"
      value -> "TRUE"
      else -> "FALSE"
    }
}
