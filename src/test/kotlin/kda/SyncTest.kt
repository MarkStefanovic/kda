@file:Suppress("SqlResolve")

package kda

import kda.domain.DbDialect
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import testutil.pgTableExists
import testutil.testPgConnection
import testutil.testSQLiteConnection
import java.sql.Connection
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.time.ExperimentalTime

data class Customer(
  val customerId: Int,
  val firstName: String,
  val lastName: String,
  val middleInitial: String?,
  val dateAdded: LocalDateTime,
  val dateUpdated: LocalDateTime?,
)

fun fetchCustomers(con: Connection, tableName: String): Set<Customer> {
  // language=PostgreSQL
  val sql = """
    |  SELECT customer_id, first_name, last_name, middle_initial, date_added, date_updated
    |  FROM sales.$tableName 
    |  ORDER BY customer_id
  """.trimMargin()
  println(
    """
    |SyncTest.fetchCustomers SQL:
    |$sql
  """.trimMargin()
  )

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
    // language=PostgreSQL
    val sql = """
      |    INSERT INTO sales.$tableName (customer_id, first_name, last_name, middle_initial, date_added, date_updated)
      |    VALUES (?, ?, ?, ?, ?, ?)
    """.trimMargin()
    println(
      """
      |SyncTest.addCustomers:
      |  SQL:
      |$sql
      |  Parameters:
      |    ${customers.joinToString("\n    ")}
    """.trimMargin()
    )

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
  // language=PostgreSQL
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
  // language=PostgreSQL
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

@ExperimentalTime
@ExperimentalStdlibApi
class SyncTest {
  @BeforeEach
  fun setup() {
    testPgConnection().use { con ->
      con.createStatement().use { stmt ->
        // language=PostgreSQL
        stmt.execute("DROP TABLE IF EXISTS sales.customer")
        // language=PostgreSQL
        stmt.execute("DROP TABLE IF EXISTS sales.customer2")
        stmt.execute(
          // language=PostgreSQL
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
      assertFalse(pgTableExists(con, schema = "sales", table = "customer2"))
    }
  }

  @Test
  fun given_no_timestamps_used() {
    testPgConnection().use { con ->
      testSQLiteConnection().use { cacheCon ->

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

        addCustomers(con = con, tableName = "customer", customer1, customer2, customer3)

        val cache = createCache(
          dialect = DbDialect.SQLite,
          con = cacheCon,
          schema = null,
        )

        val resultAfterAdd = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          showSQL = true,
          criteria = null,
        )

        val actual = fetchCustomers(con = con, tableName = "customer2")

        assertEquals(expected = customers, actual = actual)

        assertEquals(expected = 0, actual = resultAfterAdd.deleted)
        assertEquals(expected = 3, actual = resultAfterAdd.upserted)

        val updatedCustomer2 = customer2.copy(dateUpdated = LocalDateTime.of(2020, 1, 2, 3, 4, 5), middleInitial = "Z")

        // TEST UPDATE
        updateCustomer(con = con, tableName = "customer", customer = updatedCustomer2)

        val resultAfterUpdate = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          showSQL = true,
        )

        val updatedCustomers = fetchCustomers(con = con, tableName = "customer2")

        assertEquals(expected = setOf(customer1, updatedCustomer2, customer3), actual = updatedCustomers)

        assertEquals(expected = 0, actual = resultAfterUpdate.deleted)
        assertEquals(expected = 1, actual = resultAfterUpdate.upserted)

        // TEST DELETE
        deleteCustomer(con = con, tableName = "customer", customerId = 3)

        val resultAfterDelete = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          showSQL = true,
        )

        val customersAfterDelete = fetchCustomers(con = con, tableName = "customer2")

        assertEquals(expected = setOf(customer1, updatedCustomer2), actual = customersAfterDelete)

        assertEquals(expected = 1, actual = resultAfterDelete.deleted)
        assertEquals(expected = 0, actual = resultAfterDelete.upserted)
      }
    }
  }

  @Test
  fun given_timestamps_used_and_empty_inital_cache() {
    testPgConnection().use { con ->
      testSQLiteConnection().use { cacheCon ->
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

        addCustomers(con = con, tableName = "customer", customer1, customer2, customer3)

        val cache = createCache(
          dialect = DbDialect.SQLite,
          con = cacheCon,
          schema = null,
        )

        val resultAfterAdd = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val actual = fetchCustomers(con = con, tableName = "customer2")

        assertEquals(expected = customers, actual = actual)

        assertEquals(expected = 0, actual = resultAfterAdd.deleted)
        assertEquals(expected = 3, actual = resultAfterAdd.upserted)

        // TEST UPDATE
        val updatedCustomer2 = customer2.copy(
          dateUpdated = LocalDateTime.of(2020, 1, 2, 3, 4, 5),
          middleInitial = "Z"
        )
        updateCustomer(con = con, tableName = "customer", customer = updatedCustomer2)

        val resultAfterUpdate = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val updatedCustomers = fetchCustomers(con = con, tableName = "customer2")

        assertEquals(expected = 3, actual = updatedCustomers.count())

        assertEquals(expected = setOf(customer1, updatedCustomer2, customer3), actual = updatedCustomers)

        assertEquals(expected = 0, actual = resultAfterUpdate.deleted)
        assertEquals(expected = 1, actual = resultAfterUpdate.upserted)

        // TEST DELETE
        deleteCustomer(con = con, tableName = "customer", customerId = 3)

        assertEquals(expected = 2, fetchCustomers(con = con, tableName = "customer").count())

        val resultAfterDelete = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val customersAfterDelete = fetchCustomers(con = con, tableName = "customer2")

        assertEquals(expected = 2, actual = customersAfterDelete.count())

        assertEquals(expected = setOf(customer1, updatedCustomer2), actual = customersAfterDelete)

        assertEquals(expected = 1, actual = resultAfterDelete.deleted)

        assertEquals(expected = 0, actual = resultAfterDelete.upserted)
      }
    }
  }

  @Test
  fun given_duplicate_source_keys_sync_just_first_one_of_them() {
    testPgConnection().use { con ->
      testSQLiteConnection().use { cacheCon ->
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

        addCustomers(con = con, tableName = "customer", customer1, customer2, customer3, customer2dupe)

        val cache = createCache(
          dialect = DbDialect.SQLite,
          con = cacheCon,
          schema = null,
        )

        sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          srcSchema = "sales",
          srcTable = "customer",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val actual = fetchCustomers(con = con, tableName = "customer2")

        assertEquals(expected = setOf(customer1, customer2dupe, customer3), actual = actual)
      }
    }
  }
}
