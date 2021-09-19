package kda.shared

import kda.Cache
import kda.DbCache
import kda.adapter.SqliteDb
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.Path
import kotlin.io.path.deleteIfExists

fun testDbCache(): Cache {
  val fp = Path("./cache.db")
  fp.deleteIfExists()
  SqliteDb.createTables()
  return DbCache(SqliteDb)
}

fun testPgConnection(): Connection = DriverManager.getConnection(
  "jdbc:postgresql://localhost:5432/testdb",
  System.getenv("DB_USER"),
  System.getenv("DB_PASS")
)
