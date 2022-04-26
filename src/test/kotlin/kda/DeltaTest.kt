@file:Suppress("SqlResolve")

package kda

import kda.domain.DbDialect
import kda.domain.DeltaResult
import org.junit.jupiter.api.BeforeEach
import testutil.Customer
import testutil.PgCustomerRepo
import testutil.sqliteCache
import testutil.testPgConnection
import testutil.testSQLiteConnection
import java.sql.Connection
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime

private fun fetchDeltaRows(con: Connection): List<Pair<Int, String>> {
  val deltaRows = mutableListOf<Pair<Int, String>>()
  con.createStatement().use { statement ->
    statement.executeQuery(
      // language=PostgreSQL
      "SELECT customer_id, kda_op FROM sales.customer2_delta ORDER BY customer_id, kda_ts"
    ).use { resultSet ->
      while (resultSet.next()) {
        val customerId = resultSet.getInt("customer_id")
        val op = resultSet.getString("kda_op")
        val row = customerId to op
        deltaRows.add(row)
      }
    }
  }
  return deltaRows
}

@ExperimentalTime
@ExperimentalStdlibApi
class DeltaTest {
  @BeforeEach
  fun setup() {
    testPgConnection().use { con ->
      PgCustomerRepo(con = con, tableName = "customer").recreateCustomerTable()
      PgCustomerRepo(con = con, tableName = "customer2").recreateCustomer2Table()
      con.createStatement().use { statement ->
        // language=PostgreSQL
        statement.execute("DROP TABLE IF EXISTS sales.customer2_delta")
      }
    }
  }

  @Test
  fun add_rows() {
    testPgConnection().use { con ->
      val srcRepo = PgCustomerRepo(con = con, tableName = "customer")

      val dstRepo = PgCustomerRepo(con = con, tableName = "customer2")
      dstRepo.addUniqueConstraint()

      val customer1 = Customer(
        customerId = 1,
        firstName = "Bob",
        lastName = "Smith",
        middleInitial = null,
        dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
        dateUpdated = null,
      )

      val customer2 = Customer(
        customerId = 2,
        firstName = "Jane",
        lastName = "Doe",
        middleInitial = null,
        dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
        dateUpdated = LocalDateTime.of(2011, 3, 4, 5, 6, 7),
      )

      testSQLiteConnection().use { _ ->
        srcRepo.addCustomers(customer1, customer2)

        val cache = sqliteCache()

        val result = delta(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          deltaDbName = "deltaDb",
          srcDbName = "src",
          srcSchema = "sales",
          srcTable = "customer",
          dstDbName = "dst",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          showSQL = true,
          criteria = null,
        )

        val expectedResult = DeltaResult(added = 2, deleted = 0, updated = 0)

        assertEquals(expected = expectedResult, actual = result)

        val deltaRows = fetchDeltaRows(con = con)

        assertEquals(expected = setOf(1 to "I", 2 to "I"), actual = deltaRows.toSet())
      }
    }
  }

  @Test
  fun delete_rows() {
    testPgConnection().use { con ->
      val srcRepo = PgCustomerRepo(con = con, tableName = "customer")

      val dstRepo = PgCustomerRepo(con = con, tableName = "customer2")
      dstRepo.addUniqueConstraint()

      val customer1 = Customer(
        customerId = 1,
        firstName = "Bob",
        lastName = "Smith",
        middleInitial = null,
        dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
        dateUpdated = null,
      )

      val customer2 = Customer(
        customerId = 2,
        firstName = "Jane",
        lastName = "Doe",
        middleInitial = null,
        dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
        dateUpdated = LocalDateTime.of(2011, 3, 4, 5, 6, 7),
      )

      testSQLiteConnection().use {
        srcRepo.addCustomers(customer2)
        dstRepo.addCustomers(customer1, customer2)

        val cache = sqliteCache()

        val result = delta(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          deltaDbName = "deltaDb",
          srcDbName = "src",
          srcSchema = "sales",
          srcTable = "customer",
          dstDbName = "dst",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          showSQL = true,
          criteria = null,
        )

        val expectedResult = DeltaResult(added = 0, deleted = 1, updated = 0)

        assertEquals(expected = expectedResult, actual = result)

        val deltaRows = fetchDeltaRows(con = con)

        assertEquals(expected = listOf(1 to "D"), actual = deltaRows)
      }
    }
  }

  @Test
  fun update_rows() {
    testPgConnection().use { con ->
      val srcRepo = PgCustomerRepo(con = con, tableName = "customer")

      val dstRepo = PgCustomerRepo(con = con, tableName = "customer2")
      dstRepo.addUniqueConstraint()

      val customer1 = Customer(
        customerId = 1,
        firstName = "Bob",
        lastName = "Smith",
        middleInitial = null,
        dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
        dateUpdated = null,
      )

      val customer2 = Customer(
        customerId = 2,
        firstName = "Jane",
        lastName = "Doe",
        middleInitial = null,
        dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
        dateUpdated = LocalDateTime.of(2011, 3, 4, 5, 6, 7),
      )

      testSQLiteConnection().use {
        srcRepo.addCustomers(customer1, customer2)
        dstRepo.addCustomers(customer1, customer2)
        srcRepo.updateCustomer(customer2.copy(middleInitial = "Z"))

        val cache = sqliteCache()

        val result = delta(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
          deltaDbName = "deltaDb",
          srcDbName = "src",
          srcSchema = "sales",
          srcTable = "customer",
          dstDbName = "dst",
          dstSchema = "sales",
          dstTable = "customer2",
          compareFields = setOf("first_name", "last_name", "middle_initial"),
          primaryKeyFieldNames = listOf("customer_id"),
          includeFields = null,
          batchSize = 2,
          showSQL = true,
          criteria = null,
        )

        val expectedResult = DeltaResult(added = 0, deleted = 0, updated = 1)

        assertEquals(expected = expectedResult, actual = result)

        val deltaRows = fetchDeltaRows(con = con)

        assertEquals(expected = listOf(2 to "U"), actual = deltaRows)
      }
    }
  }
}
