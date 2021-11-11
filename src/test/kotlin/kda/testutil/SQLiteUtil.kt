package kda.testutil

import kda.Cache
import kda.DbCache
import kda.adapter.SQLDb
import kda.adapter.sqliteHikariDatasource
import org.jetbrains.exposed.sql.Database
import java.sql.Connection
import java.sql.DriverManager

fun testSQLiteDbConnection(): Connection =
  DriverManager.getConnection("jdbc:sqlite:file:test?mode=memory&cache=shared")

fun testSQLiteDbCache(): Cache {
  val db = SQLDb(Database.connect(sqliteHikariDatasource()))
  db.dropTables()
  db.createTables()
  return DbCache(db)
}
