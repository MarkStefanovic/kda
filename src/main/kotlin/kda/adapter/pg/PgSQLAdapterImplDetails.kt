package kda.adapter.pg

import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.SQLAdapterImplDetails
import java.time.LocalDate

class PgSQLAdapterImplDetails(private val stdImpl: StdSQLAdapterImplDetails) :
  SQLAdapterImplDetails by stdImpl {

  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "NULL"
      value -> "TRUE"
      else -> "FALSE"
    }
}
