package kda.adapter.pg

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import java.sql.Connection

@ExperimentalStdlibApi
class PgAdapter(
  con: Connection,
  showSQL: Boolean,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = PgAdapterDetails,
  )
) : Adapter by stdAdapter
