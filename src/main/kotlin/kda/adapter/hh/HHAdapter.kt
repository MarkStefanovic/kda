package kda.adapter.hh

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import java.sql.Connection
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
class HHAdapter(
  con: Connection,
  showSQL: Boolean,
  queryTimeout: Duration,
  timestampResolution: ChronoUnit,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = HHAdapterDetails,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  ),
) : Adapter by stdAdapter
