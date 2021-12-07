package testutil

import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertTrue

fun pgTableExists(con: Connection, schema: String, table: String): Boolean =
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

fun testPgConnection(): Connection {
  val config: TestConfig = getConfig()

  return DriverManager.getConnection(
    config.pgURL,
    config.pgUsername,
    config.pgPassword,
  )
}

class TableExistsUtil {
  @Test
  fun given_pg_db_when_table_exists() {
    testPgConnection().use { con ->
      con.prepareStatement("DROP TABLE IF EXISTS sales.customer").executeUpdate()

      con.prepareStatement(
        """
          CREATE TABLE sales.customer (
              customer_id SERIAL PRIMARY KEY
          ,   first_name TEXT
          ,   last_name TEXT
          )
          """
      ).executeUpdate()

      val actual = pgTableExists(con = con, schema = "sales", table = "customer")

      assertTrue(actual)
    }
  }
}
