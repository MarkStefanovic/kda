package kda

import kda.adapter.sqlite.SQLiteCache
import kda.adapter.where
import kda.domain.BoundParameter
import kda.domain.Criteria
import kda.domain.DataType
import kda.domain.DbDialect
import kda.domain.Field
import kda.domain.Parameter
import kda.domain.Table
import kda.domain.eq
import kda.testutil.pgTableExists
import kda.testutil.testPgConnection
import kda.testutil.testSQLiteConnection
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

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

@ExperimentalStdlibApi
class SyncTest {
  @BeforeEach
  fun setup() {
    testPgConnection().use { con ->
      con.createStatement().use { stmt ->
        stmt.execute("DROP TABLE IF EXISTS sales.customer")
        stmt.execute("DROP TABLE IF EXISTS sales.customer2")
        stmt.execute(
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

        val resultAfterAdd = sync(
          srcCon = con,
          dstCon = con,
          cacheCon = cacheCon,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          cacheDialect = DbDialect.SQLite,
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
          cacheCon = cacheCon,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          cacheDialect = DbDialect.SQLite,
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
          cacheCon = cacheCon,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          cacheDialect = DbDialect.SQLite,
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

        val resultAfterAdd = sync(
          srcCon = con,
          dstCon = con,
          cacheCon = cacheCon,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          cacheDialect = DbDialect.SQLite,
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
          cacheCon = cacheCon,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          cacheDialect = DbDialect.SQLite,
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
          cacheCon = cacheCon,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          cacheDialect = DbDialect.SQLite,
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

        sync(
          srcCon = con,
          dstCon = con,
          cacheCon = cacheCon,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          cacheDialect = DbDialect.SQLite,
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

@ExperimentalStdlibApi
class GetFullCriteriaTest {
  @Test
  fun given_cache_empty_and_no_initial_criteria() {
    testSQLiteConnection().use { con ->
      val cache = SQLiteCache(con = con, showSQL = false)

      val tbl = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "first_name", dataType = DataType.nullableText(null)),
          Field(name = "last_name", dataType = DataType.nullableText(null)),
          Field(name = "date_added", dataType = DataType.localDateTime),
          Field(name = "date_updated", dataType = DataType.nullableLocalDateTime),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      val actual = getFullCriteria(
        dstDialect = DbDialect.SQLite,
        dstSchema = null,
        dstTable = tbl,
        tsFieldNames = setOf("date_added", "date_updated"),
        cache = cache,
        criteria = null,
      )

      assertNull(actual)
    }
  }

  @Test
  fun given_cache_not_empty_and_no_initial_criteria() {
    testSQLiteConnection().use { con ->
      val cache = SQLiteCache(con = con, showSQL = false)

      val ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5)

      cache.addTimestamp(
        schema = null,
        table = "customer",
        fieldName = "date_added",
        ts = ts,
      )

      val tbl = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "first_name", dataType = DataType.nullableText(null)),
          Field(name = "last_name", dataType = DataType.nullableText(null)),
          Field(name = "date_added", dataType = DataType.localDateTime),
          Field(name = "date_updated", dataType = DataType.nullableLocalDateTime),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      val actual = getFullCriteria(
        dstDialect = DbDialect.SQLite,
        dstSchema = null,
        dstTable = tbl,
        tsFieldNames = setOf("date_added", "date_updated"),
        cache = cache,
        criteria = null,
      )

      val expectedSQL = """"date_added" > ?"""

      val expected = Criteria(
        dialect = DbDialect.SQLite,
        sql = expectedSQL,
        boundParameters = listOf(
          BoundParameter(
            parameter = Parameter(
              name = "date_added",
              dataType = DataType.localDateTime,
              sql = """"date_added" > ?""",
            ),
            value = ts,
          ),
        )
      )

      assertNotNull(actual)

      assertEquals(expected = expectedSQL, actual = actual.sql)

      assertEquals(expected = expected, actual = actual)
    }
  }

  @Test
  fun given_cache_empty_and_simple_initial_criteria() {
    testSQLiteConnection().use { con ->
      val cache = SQLiteCache(con = con, showSQL = false)

      val tbl = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "first_name", dataType = DataType.nullableText(null)),
          Field(name = "last_name", dataType = DataType.nullableText(null)),
          Field(name = "date_added", dataType = DataType.localDateTime),
          Field(name = "date_updated", dataType = DataType.nullableLocalDateTime),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      val initialCriteria = where(DbDialect.SQLite).and(Field(name = "customer_id", dataType = DataType.int) eq 1)

      val actual = getFullCriteria(
        dstDialect = DbDialect.SQLite,
        dstSchema = null,
        dstTable = tbl,
        tsFieldNames = setOf("date_added", "date_updated"),
        cache = cache,
        criteria = initialCriteria,
      )

      val expectedSQL = """"customer_id" = ?"""

      val expected = Criteria(
        dialect = DbDialect.SQLite,
        sql = expectedSQL,
        boundParameters = listOf(
          BoundParameter(
            parameter = Parameter(
              name = "customer_id",
              dataType = DataType.int,
              sql = """"customer_id" = ?""",
            ),
            value = 1,
          ),
        )
      )

      assertNotNull(actual)

      assertEquals(expected = expectedSQL, actual = actual.sql)

      assertEquals(expected = expected, actual = actual)
    }
  }

  @Test
  fun given_cache_not_empty_and_simple_initial_criteria() {
    testSQLiteConnection().use { con ->
      val cache = SQLiteCache(con = con, showSQL = false)

      val ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5)

      cache.addTimestamp(schema = null, table = "customer", fieldName = "date_added", ts = ts)

      val tbl = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "first_name", dataType = DataType.nullableText(null)),
          Field(name = "last_name", dataType = DataType.nullableText(null)),
          Field(name = "date_added", dataType = DataType.localDateTime),
          Field(name = "date_updated", dataType = DataType.nullableLocalDateTime),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      val initialCriteria = where(DbDialect.SQLite).and(Field(name = "customer_id", dataType = DataType.int) eq 1)

      val actual = getFullCriteria(
        dstDialect = DbDialect.SQLite,
        dstSchema = null,
        dstTable = tbl,
        tsFieldNames = setOf("date_added", "date_updated"),
        cache = cache,
        criteria = initialCriteria,
      )

      val expectedSQL = """("customer_id" = ?) AND ("date_added" > ?)"""

      val expected = Criteria(
        dialect = DbDialect.SQLite,
        sql = expectedSQL,
        boundParameters = listOf(
          BoundParameter(parameter = Parameter(name = "customer_id", dataType = DataType.int, sql = """"customer_id" = ?"""), value = 1),
          BoundParameter(parameter = Parameter(name = "date_added", dataType = DataType.localDateTime, sql = """"date_added" > ?"""), value = ts),
        )
      )

      assertNotNull(actual)

      assertEquals(expected = expectedSQL, actual = actual.sql)

      assertEquals(expected = expected, actual = actual)
    }
  }
}
