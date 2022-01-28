package kda

import kda.adapter.pg.PgCache
import kda.adapter.sqlite.SQLiteCache
import kda.domain.Cache
import kda.domain.DbDialect
import java.sql.Connection

fun createCache(
  dialect: DbDialect,
  con: Connection,
  schema: String?,
  showSQL: Boolean = false,
): Cache =
  when (dialect) {
    DbDialect.HH -> TODO()
    DbDialect.MSSQL -> TODO()
    DbDialect.PostgreSQL -> PgCache(
      con = con,
      cacheSchema = schema ?: error("cacheSchema is required"),
      showSQL = showSQL,
    )
    DbDialect.SQLite -> SQLiteCache(
      con = con,
      showSQL = showSQL,
    )
  }
