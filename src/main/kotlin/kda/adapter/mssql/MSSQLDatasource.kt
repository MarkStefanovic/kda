package kda.adapter.mssql

import kda.adapter.JdbcExecutor
import kda.domain.Datasource
import java.sql.Connection

fun mssqlDatasource(con: Connection): Datasource {
  val executor = JdbcExecutor(con = con)
  return Datasource(
    connection = con,
    adapter = msSQLAdapter,
    executor = executor,
    inspector = MSSQLInspector(sqlExecutor = executor),
  )
}
