package kda.adapter.sqlite

import kda.adapter.std.StdAdapter
import kda.domain.Adapter
import kda.domain.DbDialect
import java.sql.Connection

@ExperimentalStdlibApi
class SQLiteAdapter(
  con: Connection,
  showSQL: Boolean,
  private val stdAdapter: Adapter = StdAdapter(
    con = con,
    showSQL = showSQL,
    details = SQLiteAdapterDetails,
    dialect = DbDialect.SQLite,
  ),
) : Adapter by stdAdapter
