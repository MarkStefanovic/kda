package kda

import kda.domain.Criteria
import kda.domain.Dialect
import kda.domain.Field
import kda.domain.Operator
import kda.domain.Predicate
import kda.domain.StringType
import kda.domain.StringValue
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
        val customer =
          Customer(
            customerId = rs.getInt(1),
            firstName = rs.getString(2),
            lastName = rs.getString(3),
          )
        customers.add(customer)
      }
    }
  }
  return customers.toSet()
}

fun addCustomers(con: Connection, customers: List<Customer>) {
  for (customer in customers) {
    val sql =
      """
      INSERT INTO sales.customer (customer_id, first_name, last_name)
      VALUES (${customer.customerId}, '${customer.firstName}', '${customer.lastName}')
    """
    con.createStatement().use { stmt -> stmt.execute(sql) }
  }
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
        }
        assertFalse(tableExists(destCon, schema = "sales", table = "customer2"))
      }
    }
  }

  @Test
  fun when_dest_table_is_empty_then_all_rows_added() {
    connect().use { srcCon ->
      connect().use { destCon ->
        val customers =
          listOf(
            Customer(customerId = 1, firstName = "Mark", lastName = "Stefanovic"),
            Customer(customerId = 2, firstName = "Bob", lastName = "Smith"),
            Customer(customerId = 3, firstName = "Mandie", lastName = "Mandlebrot"),
          )
        addCustomers(con = srcCon, customers = customers)

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
            added = 3,
            deleted = 0,
            updated = 0,
          )
        assertEquals(expected = expectedSyncResult, actual = result)

        val actualRows = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = customers.toSet(), actual = actualRows)
      }
    }
  }

  @Test
  fun sync_with_criteria_should_only_refresh_rows_that_match_criteria() {
    connect().use { srcCon ->
      connect().use { destCon ->
        val customers =
          listOf(
            Customer(customerId = 1, firstName = "Mark", lastName = "Stefanovic"),
            Customer(customerId = 2, firstName = "Bob", lastName = "Smith"),
            Customer(customerId = 3, firstName = "Mandie", lastName = "Smith"),
          )
        addCustomers(con = srcCon, customers = customers)

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
            criteria = listOf(
              Criteria(
                listOf(Predicate(
                  field = Field(name = "last_name", dataType = StringType(40)),
                  operator = Operator.Equals,
                  value = StringValue(value = "Smith", maxLength = 40),
                ))
              )
            ),
          )

        val expectedSyncResult =
          SyncResult.Success(
            srcSchema = "sales",
            srcTable = "customer",
            destSchema = "sales",
            destTable = "customer2",
            added = 2,
            deleted = 0,
            updated = 0,
          )
        assertEquals(expected = expectedSyncResult, actual = result)

        val expectedRows = setOf(
          Customer(customerId = 2, firstName = "Bob", lastName = "Smith"),
          Customer(customerId = 3, firstName = "Mandie", lastName = "Smith"),
        )

        val actualRows = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = expectedRows, actual = actualRows)
      }
    }
  }
}
