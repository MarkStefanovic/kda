package kda.shared

import java.sql.Connection
import java.sql.DriverManager

fun connect(): Connection =
  DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/testdb",
    System.getenv("DB_USER"),
    System.getenv("DB_PASS")
  )

fun tableExists(con: Connection, schema: String, table: String): Boolean =
  con.createStatement().use { stmt ->
    val sql =
      """
        SELECT COUNT(*)
        FROM information_schema.tables 
        WHERE table_schema = '$schema'
        AND table_name = '$table'
        """
    stmt.executeQuery(sql).use { rs ->
      rs.next()
      rs.getInt(1) > 0
    }
  }