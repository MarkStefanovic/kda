package kda.adapter.hh

import kda.adapter.std.StdAdapterDetails

@ExperimentalStdlibApi
object HHAdapterDetails : StdAdapterDetails() {
  override fun wrapName(name: String): String = "`$name`"
}
