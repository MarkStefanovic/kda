package kda.adapter.sqlite

import kda.adapter.JdbcExecutor
import kda.domain.Datasource
import java.sql.Connection

fun sqliteDatasource(con: Connection): Datasource {
  val executor = JdbcExecutor(con = con)

  return Datasource(
    connection = con,
    adapter = sqliteAdapter,
    executor = executor,
    inspector = SQLiteInspector(sqlExecutor = executor),
  )
}
