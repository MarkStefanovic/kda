package kda.adapter.hh

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import java.sql.Connection

@ExperimentalStdlibApi
class HHAdapter(
  con: Connection,
  showSQL: Boolean,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = HHAdapterDetails,
  ),
) : Adapter by stdAdapter
