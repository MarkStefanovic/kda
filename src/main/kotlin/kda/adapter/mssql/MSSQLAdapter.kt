package kda.adapter.mssql

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import java.sql.Connection

@ExperimentalStdlibApi
class MSSQLAdapter(
  con: Connection,
  showSQL: Boolean,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = MSSQLAdapterDetails,
  )
) : Adapter by stdAdapter
