package kda.adapter.pg

import kda.adapter.std.StdAdapterDetails

@ExperimentalStdlibApi
object PgAdapterDetails : StdAdapterDetails() {
  override fun wrapName(name: String): String = "\"$name\""
}
