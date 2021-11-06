package kda

import kda.adapter.hive.hiveDatasource
import kda.adapter.mssql.mssqlDatasource
import kda.adapter.pg.pgDatasource
import kda.adapter.sqlite.sqliteDatasource
import kda.domain.Dialect
import java.sql.Connection

fun datasource(con: Connection, dialect: Dialect) = when (dialect) {
  Dialect.HortonworksHive -> hiveDatasource(con)
  Dialect.MSSQLServer -> mssqlDatasource(con)
  Dialect.PostgreSQL -> pgDatasource(con)
  Dialect.SQLite -> sqliteDatasource(con)
}
