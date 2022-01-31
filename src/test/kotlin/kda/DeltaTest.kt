@file:Suppress("SqlResolve")

package kda

import kda.domain.DbDialect
import kda.domain.DeltaResult
import org.junit.jupiter.api.BeforeEach
import testutil.Customer
import testutil.PgCustomerRepo
import testutil.testPgConnection
import testutil.testSQLiteConnection
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
class DeltaTest {
  @BeforeEach
  fun setup() {
    testPgConnection().use { con ->
      PgCustomerRepo(con = con, tableName = "customer").recreateTable()
      PgCustomerRepo(con = con, tableName = "customer2").recreateTable()
    }
  }

  @Test
  fun happy_path() {
    testPgConnection().use { con ->
      val srcRepo = PgCustomerRepo(con = con, tableName = "customer")

      val dstRepo = PgCustomerRepo(con = con, tableName = "customer2")
      dstRepo.addUniqueConstraint()

      testSQLiteConnection().use { cacheCon ->
        val customers = setOf(
          Customer(
            customerId = 1,
            firstName = "Bob",
            lastName = "Smith",
            middleInitial = null,
            dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
            dateUpdated = null,
          ),
          Customer(
            customerId = 2,
            firstName = "Jane",
            lastName = "Doe",
            middleInitial = null,
            dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
            dateUpdated = LocalDateTime.of(2011, 3, 4, 5, 6, 7),
          )
        )

        srcRepo.addCustomers(*customers.toTypedArray())

        val cache = createCache(
          dialect = DbDialect.SQLite,
          con = cacheCon,
          schema = null,
        )

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

        val expected = DeltaResult(
          added = 2,
          deleted = 0,
          updated = 0,
        )

        assertEquals(expected = expected, actual = result)

        val deltaRows = mutableListOf<Triple<Int, LocalDateTime, String>>()
        con.createStatement().use { statement ->
          val result = statement.executeQuery(
            // language=PostgreSQL
            "SELECT customer_id, batch_ts, op FROM sales.customer2_delta"
          ).use { resultSet ->
            while (resultSet.next()) {
              val customerId = resultSet.getInt("customer_id")
              val batchTs = resultSet.getTimestamp("batch_ts").toLocalDateTime()
              val op = resultSet.getString("op")
              val row = Triple(customerId, batchTs, op)
              deltaRows.add(row)
            }
          }
        }

        assertEquals(expected = setOf(), actual = deltaRows.toSet())
      }
    }
  }
}
