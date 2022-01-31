package kda.domain

import java.sql.Connection

interface Cache {
  fun addTable(dbName: String, schema: String?, table: Table)

  fun getTable(dbName: String, schema: String?, table: String): Table?

  fun tableExists(con: Connection, dbName: String, schema: String?, table: String): Boolean
}
