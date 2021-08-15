package adapter.pg

import adapter.std.StdSQLAdapterImplDetails
import domain.SQLAdapterImplDetails

class PgSQLAdapterImplDetails(private val stdImpl: StdSQLAdapterImplDetails) :
  SQLAdapterImplDetails by stdImpl {

  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "NULL"
      value -> "TRUE"
      else -> "FALSE"
    }
}
