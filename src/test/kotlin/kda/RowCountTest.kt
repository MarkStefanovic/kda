@file:Suppress("SqlResolve")

package kda

import kda.domain.DbDialect
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import testutil.Customer
import testutil.PgCustomerRepo
import testutil.testPgConnection
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
class RowCountTest {
  @BeforeEach
  fun setup() {
    testPgConnection().use { con ->
      PgCustomerRepo(con = con, tableName = "customer").recreateTable()
    }
  }

  @Test
  fun happy_path() {
    testPgConnection().use { con ->
      val repo = PgCustomerRepo(con = con, tableName = "customer")

      repo.addCustomers(
        Customer(
          customerId = 1,
          firstName = "Mark",
          lastName = "Stefanovic",
          middleInitial = "E",
          dateAdded = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
          dateUpdated = null,
        ),
        Customer(
          customerId = 2,
          firstName = "Bob",
          lastName = "Smith",
          middleInitial = null,
          dateAdded = LocalDateTime.of(2011, 2, 3, 4, 5, 6),
          dateUpdated = LocalDateTime.of(2012, 3, 4, 5, 6, 7),
        ),
        Customer(
          customerId = 3,
          firstName = "Mandie",
          lastName = "Mandlebrot",
          middleInitial = "M",
          dateAdded = LocalDateTime.of(2013, 4, 5, 6, 7, 8),
          dateUpdated = null,
        ),
      )

      val actual = getRowCount(con = con, dialect = DbDialect.PostgreSQL, schema = "sales", table = "customer")

      assertEquals(expected = 3, actual = actual)
    }
  }
}
