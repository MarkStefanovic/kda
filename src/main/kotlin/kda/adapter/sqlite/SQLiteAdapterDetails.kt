package kda.adapter.sqlite

import kda.adapter.std.StdAdapterDetails

@ExperimentalStdlibApi
object SQLiteAdapterDetails : StdAdapterDetails() {
  override fun wrapName(name: String): String = "\"$name\""
}
