package kda.adapter

import kda.adapter.hh.HHAdapter
import kda.adapter.mssql.MSSQLAdapter
import kda.adapter.pg.PgAdapter
import kda.adapter.sqlite.SQLiteAdapter
import kda.domain.Adapter
import kda.domain.DbDialect
import java.sql.Connection
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun selectAdapter(
  dialect: DbDialect,
  con: Connection,
  showSQL: Boolean,
  queryTimeout: Duration,
  timestampResolution: ChronoUnit,
): Adapter =
  when (dialect) {
    DbDialect.HH -> HHAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout, timestampResolution = timestampResolution)
    DbDialect.MSSQL -> MSSQLAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout, timestampResolution = timestampResolution)
    DbDialect.PostgreSQL -> PgAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout, timestampResolution = timestampResolution)
    DbDialect.SQLite -> SQLiteAdapter(con = con, showSQL = showSQL, queryTimeout = queryTimeout, timestampResolution = timestampResolution)
  }
