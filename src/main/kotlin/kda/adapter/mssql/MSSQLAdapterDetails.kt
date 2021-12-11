package kda.adapter.mssql

import kda.adapter.std.StdAdapterDetails
import kda.domain.DbAdapterDetails

@ExperimentalStdlibApi
object MSSQLAdapterDetails : DbAdapterDetails by StdAdapterDetails {
  override fun wrapName(name: String): String = "[$name]"
}
