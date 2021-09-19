package kda

import kda.adapter.SqliteDb
import kotlin.io.path.Path
import kotlin.io.path.deleteIfExists

fun testDbCache(): Cache {
  val fp = Path("./cache.db")
  fp.deleteIfExists()
  SqliteDb.createTables()
  return DbCache(SqliteDb)
}
