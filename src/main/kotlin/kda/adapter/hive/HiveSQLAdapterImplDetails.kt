package kda.adapter.hive

import kda.domain.SQLAdapterImplDetails

class HiveSQLAdapterImplDetails(private val std: SQLAdapterImplDetails) : SQLAdapterImplDetails by std {
  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "NULL"
      value -> "TRUE"
      else -> "FALSE"
    }

  override fun wrapName(name: String) =
    "`${name.lowercase()}`"
}
