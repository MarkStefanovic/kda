package kda.testutil

import kda.Cache
import kda.DbCache
import kda.adapter.SQLDb
import org.jetbrains.exposed.sql.Database
import java.sql.Connection
import java.sql.DriverManager

fun testSQLiteDbConnection(): Connection =
  DriverManager.getConnection("jdbc:sqlite:file:test?mode=memory&cache=shared")

fun testSQLiteDbCache(): Cache {
  val sqlitePath = "jdbc:sqlite:file:test?mode=memory&cache=shared"
  DriverManager.getConnection(sqlitePath) // needed to keep database alive
  val exposedDb = Database.connect(sqlitePath)
  val db = SQLDb(exposedDb,true)
  db.dropTables()
  db.createTables()
  return DbCache(exposedDb, true)
}
