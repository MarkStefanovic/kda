package kda.adapter.pg

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
class PgAdapter(
  con: Connection,
  showSQL: Boolean,
  queryTimeout: Duration,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = PgAdapterDetails,
    queryTimeout = queryTimeout,
  )
) : Adapter by stdAdapter
