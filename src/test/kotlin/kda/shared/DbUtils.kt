package kda.shared

import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.test.assertTrue

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

class TableExistsUtil {
  @Test
  fun tableExists_happy_path() {
    testPgConnection().use { con ->
      con.createStatement().use { stmt ->
        stmt.execute("DROP TABLE IF EXISTS sales.customer")
        stmt.execute(
          """
            CREATE TABLE sales.customer (
                customer_id SERIAL PRIMARY KEY
            ,   first_name TEXT
            ,   last_name TEXT
            )
            """
        )
      }
      val actual = tableExists(con = con, schema = "sales", table = "customer")
      assertTrue(actual)
    }
  }
}
