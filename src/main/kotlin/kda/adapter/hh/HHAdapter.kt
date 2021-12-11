package kda.adapter.hh

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import kda.domain.DbDialect
import java.sql.Connection

@ExperimentalStdlibApi
class HHAdapter(
  con: Connection,
  showSQL: Boolean,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = HHAdapterDetails,
    dialect = DbDialect.HH,
  ),
) : Adapter by stdAdapter
