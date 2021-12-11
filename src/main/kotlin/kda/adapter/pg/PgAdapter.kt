package kda.adapter.pg

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import kda.domain.DbDialect
import java.sql.Connection

@ExperimentalStdlibApi
class PgAdapter(
  con: Connection,
  showSQL: Boolean,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = PgAdapterDetails,
    dialect = DbDialect.PostgreSQL,
  )
) : Adapter by stdAdapter
