package kda.adapter

import kda.adapter.hh.HHAdapter
import kda.adapter.mssql.MSSQLAdapter
import kda.adapter.pg.PgAdapter
import kda.adapter.sqlite.SQLiteAdapter
import kda.domain.Adapter
import kda.domain.DbDialect
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun selectAdapter(
  dialect: DbDialect,
  con: Connection,
  showSQL: Boolean,
  queryTimeout: Duration,
): Adapter =
  when (dialect) {
    DbDialect.HH -> HHAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout)
    DbDialect.MSSQL -> MSSQLAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout)
    DbDialect.PostgreSQL -> PgAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout)
    DbDialect.SQLite -> SQLiteAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout)
  }
