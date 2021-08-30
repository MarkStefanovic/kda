package kda

import kda.domain.Dialect
import kda.domain.SyncResult
import kda.shared.connect
import kda.shared.tableExists
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertFalse

data class Customer(val customerId: Int, val firstName: String, val lastName: String)

fun fetchCustomers(con: Connection, tableName: String): Set<Customer> {
  val sql = "SELECT customer_id, first_name, last_name FROM sales.$tableName ORDER BY customer_id"
  val customers = mutableListOf<Customer>()
  con.createStatement().use { stmt ->
    stmt.executeQuery(sql).use { rs ->
      while (rs.next()) {
        val customer = Customer(
          customerId = rs.getInt(0),
          firstName = rs.getString(1),
          lastName = rs.getString(2),
        )
        customers.add(customer)
      }
    }
  }
  return customers.toSet()
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SyncTest {
  @BeforeEach
  fun setup() {
    connect().use { srcCon ->
      connect().use { destCon ->
        srcCon.createStatement().use { stmt ->
          stmt.execute("DROP TABLE IF EXISTS sales.customer")
          stmt.execute("DROP TABLE IF EXISTS sales.customer2")
          stmt.execute(
            """
            CREATE TABLE sales.customer (
                customer_id SERIAL PRIMARY KEY
            ,   first_name TEXT
            ,   last_name TEXT
            )
            """.trimIndent()
          )
          stmt.execute(
            """
            INSERT INTO sales.customer (customer_id, first_name, last_name)
            VALUES 
                (1, 'Mark', 'Stefanovic')
            ,   (2, 'Bob', 'Smith')
            ,   (3, 'Mandie', 'Mandlebrot')
            ,   (4, 'Mark', 'Smith')
            ,   (5, 'Jenny', 'Smith')
            """
          )
        }
        assertFalse(tableExists(destCon, schema = "sales", table = "customer2"))
      }
    }
  }

  @Test
  fun when_dest_table_is_empty_then_all_rows_added() {
    connect().use { srcCon ->
      connect().use { destCon ->
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

        val expectedSyncResult =
          SyncResult.Success(
            srcSchema = "sales",
            srcTable = "customer",
            destSchema = "sales",
            destTable = "customer2",
            added = 5,
            deleted = 0,
            updated = 0,
          )
        assertEquals(expected = expectedSyncResult, actual = result)

        val expectedRows = setOf(
          Customer(
            customerId = 1,
            firstName = "Mark",
            lastName = "Stefanovic",
          ),
          Customer(
            customerId = 2,
            firstName = "Bob",
            lastName = "Smith",
          ),
          Customer(
            customerId = 3,
            firstName = "Mandie",
            lastName = "Mandlebrot",
          ),
          Customer(
            customerId = 4,
            firstName = "Mark",
            lastName = "Smith",
          ),
          Customer(
            customerId = 5,
            firstName = "Jenny",
            lastName = "Smith",
          ),
        )

        val actualRows = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = expectedRows, actual = actualRows)
      }
    }
  }
}
