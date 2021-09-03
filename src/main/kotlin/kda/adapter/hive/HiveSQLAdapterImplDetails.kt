package kda.adapter.hive

import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.SQLAdapterImplDetails

class HiveSQLAdapterImplDetails(private val stdImpl: StdSQLAdapterImplDetails) :
  SQLAdapterImplDetails by stdImpl {

  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "NULL"
      value -> "TRUE"
      else -> "FALSE"
    }
}
