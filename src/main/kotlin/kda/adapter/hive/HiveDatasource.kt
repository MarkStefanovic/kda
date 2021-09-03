package kda.adapter.hive

import kda.adapter.JdbcExecutor
import kda.domain.Datasource
import java.sql.Connection

fun hiveDatasource(con: Connection): Datasource {
  val executor = JdbcExecutor(con)
  return Datasource(
    connection = con,
    adapter = hiveSQLAdapter,
    executor = executor,
    inspector = HiveInspector(con),
  )
}
