package service

import domain.Dialect
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CopyTableTest {
  private fun connect(): Connection =
    DriverManager.getConnection(
      "jdbc:postgresql://localhost:5432/testdb",
      System.getenv("DB_USER"),
      System.getenv("DB_PASS")
    )

  private fun tableExists(con: Connection, schema: String, table: String): Boolean =
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

  @Test
  fun copyTable_happy_path() {
    connect().use { srcCon: Connection ->
      connect().use { destCon: Connection ->
        destCon.createStatement().use { stmt ->
          stmt.execute("DROP TABLE IF EXISTS sales.customer")
        }
        destCon.createStatement().use { stmt ->
          val sql =
            """
            CREATE TABLE sales.customer (
                customer_id SERIAL PRIMARY KEY
            ,   first_name TEXT
            ,   last_name TEXT
            ,   dob DATE
            )
            """
          stmt.execute(sql)
        }

        destCon.createStatement().use { stmt ->
          stmt.execute("DROP TABLE IF EXISTS sales.customer2")
        }
        assert(!tableExists(destCon, "sales", "customer2"))

        val result =
          copyTable(
            srcCon = srcCon,
            destCon = destCon,
            srcDialect = Dialect.PostgreSQL,
            destDialect = Dialect.PostgreSQL,
            srcSchema = "sales",
            srcTable = "customer",
            destSchema = "sales",
            destTable = "customer2",
            includeFields = setOf("customer_id", "first_name", "last_name", "dob"),
            primaryKeyFields = listOf("customer_id"),
          )
        assert(result.created)
        assertEquals(
          expected = setOf("customer_id", "first_name", "last_name", "dob"),
          actual = result.table.fields.map { fld -> fld.name }.toSet()
        )
        assert(tableExists(destCon, "sales", "customer2"))
      }
    }
  }
}
