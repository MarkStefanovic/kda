package kda.adapter.hh

import kda.adapter.std.StdAdapterDetails
import kda.domain.DbAdapterDetails

@ExperimentalStdlibApi
object HHAdapterDetails : DbAdapterDetails by StdAdapterDetails {
  override fun wrapName(name: String): String = "`$name`"
}
