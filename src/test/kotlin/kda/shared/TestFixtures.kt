package kda.shared

import kda.Cache
import kda.DbCache
import kda.adapter.SqliteDb
import kda.adapter.sqliteDatasource
import java.sql.Connection
import java.sql.DriverManager

fun testDbCache(): Cache {
  val db = SqliteDb(sqliteDatasource())
  db.dropTables()
  db.createTables()
  return DbCache(db)
}

fun testPgConnection(): Connection = DriverManager.getConnection(
  "jdbc:postgresql://localhost:5432/testdb",
  System.getenv("DB_USER"),
  System.getenv("DB_PASS")
)
