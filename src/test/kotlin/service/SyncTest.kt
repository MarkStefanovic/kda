package service

import domain.Dialect
import domain.SyncResult
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import shared.connect
import shared.tableExists
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SyncTest {
  @Test
  fun when_dest_table_is_empty_then_all_rows_added() {
    connect().use { srcCon ->
      connect().use { destCon ->
        srcCon.createStatement().use { stmt ->
          stmt.execute("DROP TABLE IF EXISTS sales.customer")
          stmt.execute("DROP TABLE IF EXISTS sales.customer2")
          stmt.execute(
            "CREATE TABLE sales.customer (customer_id SERIAL PRIMARY KEY, first_name TEXT, last_name TEXT)"
          )
          stmt.execute(
            """
            INSERT INTO sales.customer (customer_id, first_name, last_name)
            VALUES 
                (1, 'Mark', 'Stefanovic')
            ,   (2, 'Bob', 'Smith')
            ,   (3, 'Mandie', 'Mandlebrot')
            """
          )
        }

        assertFalse(tableExists(destCon, schema = "sales", table = "customer2"))

        val result =
          sync(
            srcCon = srcCon,
            destCon = destCon,
            srcDialect = Dialect.PostgreSQL,
            destDialect = Dialect.PostgreSQL,
            srcSchema = "sales",
            srcTable = "customer",
            destSchema = "sales",
            destTable = "customer2",
            compareFields = setOf("first_name"),
            primaryKeyFieldNames = listOf("customer_id"),
            includeFields = null,
          )

        val expectedSyncResult = SyncResult.Success(
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          added = 3,
          deleted = 0,
          updated = 0,
        )
        assertEquals(expected = expectedSyncResult, actual = result)

        destCon.createStatement().use { stmt ->
          val rs = stmt.executeQuery("SELECT * FROM sales.customer2 ORDER BY first_name")
          val rows = mutableListOf<Map<String, Any>>()
          while (rs.next()) {
            val row = mapOf(
              "customer_id" to rs.getInt("customer_id"),
              "first_name" to rs.getString("first_name"),
              "last_name" to rs.getString("last_name"),
            )
            rows.add(row)
          }
          val expected = listOf(
            mapOf(
              "customer_id" to 2,
              "first_name" to "Bob",
              "last_name" to "Smith",
            ),
            mapOf(
              "customer_id" to 3,
              "first_name" to "Mandie",
              "last_name" to "Mandlebrot",
            ),
            mapOf(
              "customer_id" to 1,
              "first_name" to "Mark",
              "last_name" to "Stefanovic",
            ),
          )
          assertEquals(expected = expected, actual = rows.toList())
        }
      }
    }
  }
}
