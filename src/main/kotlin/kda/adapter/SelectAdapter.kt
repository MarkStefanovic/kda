package kda.adapter

import kda.adapter.hh.HHAdapter
import kda.adapter.mssql.MSSQLAdapter
import kda.adapter.pg.PgAdapter
import kda.adapter.sqlite.SQLiteAdapter
import kda.domain.Adapter
import kda.domain.DbDialect
import java.sql.Connection

@ExperimentalStdlibApi
fun selectAdapter(dialect: DbDialect, con: Connection, showSQL: Boolean): Adapter =
  when (dialect) {
    DbDialect.HH -> HHAdapter(con = con, showSQL = showSQL)
    DbDialect.MSSQL -> MSSQLAdapter(con = con, showSQL = showSQL)
    DbDialect.PostgreSQL -> PgAdapter(con = con, showSQL = showSQL)
    DbDialect.SQLite -> SQLiteAdapter(con = con, showSQL = showSQL)
  }
