package kda.adapter.hive

import kda.domain.SQLAdapterImplDetails

class HiveSQLAdapterImplDetails(private val stdImpl: SQLAdapterImplDetails) :
  SQLAdapterImplDetails by stdImpl {

  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "NULL"
      value -> "TRUE"
      else -> "FALSE"
    }

  override fun wrapName(name: String) = "`${name.lowercase()}`"
}
