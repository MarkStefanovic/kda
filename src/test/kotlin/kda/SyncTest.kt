package kda

import kda.domain.Dialect
import kda.shared.tableExists
import kda.shared.testDbCache
import kda.shared.testPgConnection
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFalse

data class Customer(
  val customerId: Int,
  val firstName: String,
  val lastName: String,
  val middleInitial: String?,
  val dateAdded: LocalDateTime,
  val dateUpdated: LocalDateTime?,
)

fun fetchCustomers(con: Connection, tableName: String): Set<Customer> {
  val sql = """
    SELECT customer_id, first_name, last_name, middle_initial, date_added, date_updated
    FROM sales.$tableName 
    ORDER BY customer_id
  """
  val customers = mutableListOf<Customer>()
  con.createStatement().use { stmt ->
    stmt.executeQuery(sql).use { rs ->
      while (rs.next()) {
        val dateUpdatedObj = rs.getObject("date_updated")
        val dateUpdated = if (dateUpdatedObj == null) {
          null
        } else {
          (dateUpdatedObj as Timestamp).toLocalDateTime()
        }
        val customer =
          Customer(
            customerId = rs.getInt("customer_id"),
            firstName = rs.getString("first_name"),
            lastName = rs.getString("last_name"),
            middleInitial = rs.getObject("middle_initial") as String?,
            dateAdded = rs.getTimestamp("date_added").toLocalDateTime(),
            dateUpdated = dateUpdated,
          )
        customers.add(customer)
      }
    }
  }
  return customers.toSet()
}

fun addCustomers(con: Connection, tableName: String, vararg customers: Customer) {
  for (customer in customers) {
    val sql = """
      INSERT INTO sales.$tableName (customer_id, first_name, last_name, middle_initial, date_added, date_updated)
      VALUES (?, ?, ?, ?, ?, ?)
    """
    con.prepareStatement(sql).use { stmt ->
      stmt.setInt(1, customer.customerId)
      stmt.setString(2, customer.firstName)
      stmt.setString(3, customer.lastName)
      if (customer.middleInitial == null) {
        stmt.setNull(4, Types.VARCHAR)
      } else {
        stmt.setString(4, customer.middleInitial)
      }
      stmt.setTimestamp(5, Timestamp.valueOf(customer.dateAdded))
      if (customer.dateUpdated == null) {
        stmt.setNull(6, Types.TIMESTAMP)
      } else {
        stmt.setTimestamp(6, Timestamp.valueOf(customer.dateUpdated))
      }
      stmt.execute()
    }
  }
}

fun deleteCustomer(con: Connection, tableName: String, customerId: Int) {
  val sql = """
    DELETE FROM sales.$tableName 
    WHERE customer_id = ?
  """
  con.prepareStatement(sql).use { stmt ->
    stmt.setInt(1, customerId)
    stmt.execute()
  }
}

fun updateCustomer(
  con: Connection,
  tableName: String,
  customer: Customer,
) {
  val sql = """
    UPDATE sales.$tableName 
    SET
      first_name = ?
    , last_name = ?
    , middle_initial = ?
    , date_added = ?
    , date_updated = ?
    WHERE 
      customer_id = ?
  """
  con.prepareStatement(sql).use { stmt ->
    stmt.setString(1, customer.firstName)
    stmt.setString(2, customer.lastName)
    if (customer.middleInitial == null) {
      stmt.setNull(3, Types.VARCHAR)
    } else {
      stmt.setString(3, customer.middleInitial)
    }
    stmt.setTimestamp(4, Timestamp.valueOf(customer.dateAdded))
    if (customer.dateUpdated == null) {
      stmt.setNull(5, Types.TIMESTAMP)
    } else {
      stmt.setTimestamp(5, Timestamp.valueOf(customer.dateUpdated))
    }
    stmt.setInt(6, customer.customerId)
    stmt.execute()
  }
}

class SyncTest {
  @BeforeEach
  fun setup() {
    testPgConnection().use { srcCon ->
      testPgConnection().use { destCon ->
        srcCon.createStatement().use { stmt ->
          stmt.execute("DROP TABLE IF EXISTS sales.customer")
          stmt.execute("DROP TABLE IF EXISTS sales.customer2")
          stmt.execute(
//            """
//            CREATE TABLE sales.customer (
//                customer_id SERIAL PRIMARY KEY
//            ,   first_name TEXT NOT NULL
//            ,   last_name TEXT NOT NULL
//            ,   middle_initial TEXT NULL
//            ,   date_added TIMESTAMP NOT NULL DEFAULT now()
//            ,   date_updated TIMESTAMP NULL
//            )
//            """
            """
            CREATE TABLE sales.customer (
                customer_id INT NOT NULL
            ,   first_name TEXT NOT NULL
            ,   last_name TEXT NOT NULL
            ,   middle_initial TEXT NULL
            ,   date_added TIMESTAMP NOT NULL DEFAULT now()
            ,   date_updated TIMESTAMP NULL
            )
            """
          )
        }
        assertFalse(tableExists(destCon, schema = "sales", table = "customer2"))
      }
    }
  }

  @Test
  fun given_no_timestamps_used() {
    testPgConnection().use { srcCon ->
      testPgConnection().use { destCon ->
        // TEST ADD
        val customer1 = Customer(
          customerId = 1,
          firstName = "Mark",
          lastName = "Stefanovic",
          middleInitial = "E",
          dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
          dateUpdated = null,
        )
        val customer2 = Customer(
          customerId = 2,
          firstName = "Bob",
          lastName = "Smith",
          middleInitial = null,
          dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
          dateUpdated = LocalDateTime.of(2012, 3, 4, 5, 6, 7),
        )
        val customer3 = Customer(
          customerId = 3,
          firstName = "Mandie",
          lastName = "Mandlebrot",
          middleInitial = "M",
          dateAdded = LocalDateTime.of(2013, 4, 5, 6, 7, 8),
          dateUpdated = null,
        )
        val customers = setOf(customer1, customer2, customer3)

        addCustomers(con = srcCon, tableName = "customer", customer1, customer2, customer3)

        val resultAfterAdd = sync(
          srcCon = srcCon,
          destCon = destCon,
          srcDialect = Dialect.PostgreSQL,
          destDialect = Dialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          cache = testDbCache(),
          chunkSize = 2,
        ).getOrThrow()

        val actual = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = customers, actual = actual)

        assertEquals(
          expected = mapOf<Map<String, Any?>, Map<String, Any?>>(
            mapOf("customer_id" to 1) to mapOf(
              "customer_id" to 1,
              "first_name" to "Mark",
              "last_name" to "Stefanovic",
              "middle_initial" to "E",
            ),
            mapOf("customer_id" to 2) to mapOf(
              "customer_id" to 2,
              "first_name" to "Bob",
              "last_name" to "Smith",
              "middle_initial" to null,
            ),
            mapOf("customer_id" to 3) to mapOf(
              "customer_id" to 3,
              "first_name" to "Mandie",
              "last_name" to "Mandlebrot",
              "middle_initial" to "M",
            ),
          ),
          actual = resultAfterAdd.added.toMap(),
        )

        assertEquals(expected = emptyMap(), actual = resultAfterAdd.deleted.toMap())

        assertEquals(expected = emptyMap(), actual = resultAfterAdd.updated.toMap())

        val updatedCustomer2 = customer2.copy(
          dateUpdated = LocalDateTime.of(2020, 1, 2, 3, 4, 5),
          middleInitial = "Z"
        )

        // TEST UPDATE
        updateCustomer(con = srcCon, tableName = "customer", customer = updatedCustomer2)

        val resultAfterUpdate = sync(
          srcCon = srcCon,
          destCon = destCon,
          srcDialect = Dialect.PostgreSQL,
          destDialect = Dialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          cache = testDbCache(),
          chunkSize = 2,
        ).getOrThrow()

        val updatedCustomers = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = setOf(customer1, updatedCustomer2, customer3), actual = updatedCustomers)

        assertEquals(expected = emptyMap(), actual = resultAfterUpdate.added.toMap())

        assertEquals(expected = emptyMap(), actual = resultAfterUpdate.deleted.toMap())

        assertEquals(
          expected = mapOf<Map<String, Any?>, Map<String, Any?>>(
            mapOf("customer_id" to 2) to mapOf(
              "customer_id" to 2,
              "first_name" to "Bob",
              "last_name" to "Smith",
              "middle_initial" to "Z",
            ),
          ),
          actual = resultAfterUpdate.updated.toMap(),
        )

        // TEST DELETE
        deleteCustomer(con = srcCon, tableName = "customer", customerId = 3)

        val resultAfterDelete = sync(
          srcCon = srcCon,
          destCon = destCon,
          srcDialect = Dialect.PostgreSQL,
          destDialect = Dialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          cache = testDbCache(),
          chunkSize = 2,
        ).getOrThrow()

        val customersAfterDelete = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = setOf(customer1, updatedCustomer2), actual = customersAfterDelete)

        assertEquals(expected = emptyMap(), actual = resultAfterDelete.added.toMap())

        assertEquals(
          expected = mapOf<Map<String, Any?>, Map<String, Any?>>(
            mapOf("customer_id" to 3) to mapOf(
              "customer_id" to 3,
              "first_name" to "Mandie",
              "last_name" to "Mandlebrot",
              "middle_initial" to "M",
            ),
          ),
          actual = resultAfterDelete.deleted.toMap(),
        )

        assertEquals(expected = emptyMap(), actual = resultAfterDelete.updated.toMap())
      }
    }
  }

  @Test
  fun given_timestamps_used() {
    testPgConnection().use { srcCon ->
      testPgConnection().use { destCon ->
        // TEST ADD
        val customer1 = Customer(
          customerId = 1,
          firstName = "Mark",
          lastName = "Stefanovic",
          middleInitial = "E",
          dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
          dateUpdated = null,
        )
        val customer2 = Customer(
          customerId = 2,
          firstName = "Bob",
          lastName = "Smith",
          middleInitial = null,
          dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
          dateUpdated = LocalDateTime.of(2012, 3, 4, 5, 6, 7),
        )
        val customer3 = Customer(
          customerId = 3,
          firstName = "Mandie",
          lastName = "Mandlebrot",
          middleInitial = "M",
          dateAdded = LocalDateTime.of(2013, 4, 5, 6, 7, 8),
          dateUpdated = null,
        )
        val customers = setOf(customer1, customer2, customer3)

        addCustomers(con = srcCon, tableName = "customer", customer1, customer2, customer3)

        val resultAfterAdd = sync(
          srcCon = srcCon,
          destCon = destCon,
          srcDialect = Dialect.PostgreSQL,
          destDialect = Dialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          cache = testDbCache(),
          chunkSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        ).getOrThrow()

        val actual = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = customers, actual = actual)

        assertEquals(
          expected = mapOf<Map<String, Any?>, Map<String, Any?>>(
            mapOf("customer_id" to 1) to mapOf(
              "customer_id" to 1,
              "first_name" to "Mark",
              "last_name" to "Stefanovic",
              "middle_initial" to "E",
              "date_added" to LocalDateTime.of(2010, 1, 2, 3, 4, 5),
              "date_updated" to null,
            ),
            mapOf("customer_id" to 2) to mapOf(
              "customer_id" to 2,
              "first_name" to "Bob",
              "last_name" to "Smith",
              "middle_initial" to null,
              "date_added" to LocalDateTime.of(2011, 2, 3, 4, 5, 6),
              "date_updated" to LocalDateTime.of(2012, 3, 4, 5, 6, 7),
            ),
            mapOf("customer_id" to 3) to mapOf(
              "customer_id" to 3,
              "first_name" to "Mandie",
              "last_name" to "Mandlebrot",
              "middle_initial" to "M",
              "date_added" to LocalDateTime.of(2013, 4, 5, 6, 7, 8),
              "date_updated" to null,
            ),
          ),
          actual = resultAfterAdd.added.toMap(),
        )

        assertEquals(expected = emptyMap(), actual = resultAfterAdd.deleted.toMap())

        assertEquals(expected = emptyMap(), actual = resultAfterAdd.updated.toMap())

        // TEST UPDATE
        val updatedCustomer2 = customer2.copy(
          dateUpdated = LocalDateTime.of(2020, 1, 2, 3, 4, 5),
          middleInitial = "Z"
        )
        updateCustomer(con = srcCon, tableName = "customer", customer = updatedCustomer2)

        val resultAfterUpdate = sync(
          srcCon = srcCon,
          destCon = destCon,
          srcDialect = Dialect.PostgreSQL,
          destDialect = Dialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          cache = testDbCache(),
          chunkSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
        ).getOrThrow()

        val updatedCustomers = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = setOf(customer1, updatedCustomer2, customer3), actual = updatedCustomers)

        assertEquals(expected = emptyMap(), actual = resultAfterUpdate.updated.toMap())

        assertEquals(expected = emptyMap(), actual = resultAfterUpdate.deleted.toMap())

        assertEquals(
          expected = mapOf<Map<String, Any?>, Map<String, Any?>>(
            mapOf("customer_id" to 2) to mapOf(
              "customer_id" to 2,
              "first_name" to "Bob",
              "last_name" to "Smith",
              "middle_initial" to "Z",
              "date_added" to LocalDateTime.of(2011, 2, 3, 4, 5, 6),
              "date_updated" to LocalDateTime.of(2020, 1, 2, 3, 4, 5),
            ),
          ),
          actual = resultAfterUpdate.added.toMap(),
        )

        // TEST DELETE
        deleteCustomer(con = srcCon, tableName = "customer", customerId = 3)

        val resultAfterDelete = sync(
          srcCon = srcCon,
          destCon = destCon,
          srcDialect = Dialect.PostgreSQL,
          destDialect = Dialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          cache = testDbCache(),
          chunkSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
        ).getOrThrow()

        val customersAfterDelete = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = setOf(customer1, updatedCustomer2), actual = customersAfterDelete)

        assertEquals(expected = emptyMap(), actual = resultAfterDelete.added.toMap())

        assertEquals(
          expected = mapOf<Map<String, Any?>, Map<String, Any?>>(
            mapOf("customer_id" to 3) to mapOf(
              "customer_id" to 3,
              "first_name" to "Mandie",
              "last_name" to "Mandlebrot",
              "middle_initial" to "M",
              "date_added" to LocalDateTime.of(2013, 4, 5, 6, 7, 8),
              "date_updated" to null,
            ),
          ),
          actual = resultAfterDelete.deleted.toMap(),
        )

        assertEquals(expected = emptyMap(), actual = resultAfterDelete.updated.toMap())
      }
    }
  }

  @Test
  fun given_duplicate_source_keys_sync_just_first_one_of_them() {
    testPgConnection().use { srcCon ->
      testPgConnection().use { destCon ->
        // TEST ADD
        val customer1 = Customer(
          customerId = 1,
          firstName = "Mark",
          lastName = "Stefanovic",
          middleInitial = "E",
          dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
          dateUpdated = null,
        )
        val customer2 = Customer(
          customerId = 2,
          firstName = "Bob",
          lastName = "Smith",
          middleInitial = null,
          dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
          dateUpdated = LocalDateTime.of(2012, 3, 4, 5, 6, 7),
        )
        val customer2dupe = Customer(
          customerId = 2,
          firstName = "Bob",
          lastName = "Smith",
          middleInitial = "X",
          dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
          dateUpdated = LocalDateTime.of(2012, 3, 4, 5, 6, 7),
        )
        val customer3 = Customer(
          customerId = 3,
          firstName = "Mandie",
          lastName = "Mandlebrot",
          middleInitial = "M",
          dateAdded = LocalDateTime.of(2013, 4, 5, 6, 7, 8),
          dateUpdated = null,
        )

        addCustomers(con = srcCon, tableName = "customer", customer1, customer2, customer3, customer2dupe)

        val result = sync(
          srcCon = srcCon,
          destCon = destCon,
          srcDialect = Dialect.PostgreSQL,
          destDialect = Dialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          destSchema = "sales",
          destTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          cache = testDbCache(),
          chunkSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
          trustPk = false,
        ).getOrThrow()

        val actual = fetchCustomers(con = destCon, tableName = "customer2")

        assertEquals(expected = setOf(customer1, customer2dupe, customer3), actual = actual)
      }
    }
  }
}
