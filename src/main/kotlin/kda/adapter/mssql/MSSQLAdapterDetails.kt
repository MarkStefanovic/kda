package kda.adapter.mssql

import kda.adapter.std.StdAdapterDetails

@ExperimentalStdlibApi
object MSSQLAdapterDetails : StdAdapterDetails() {

  override fun wrapName(name: String): String = "[$name]"
}
