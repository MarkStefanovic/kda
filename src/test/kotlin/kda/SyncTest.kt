@file:Suppress("SqlResolve")

package kda

import kda.domain.DbDialect
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import testutil.Customer
import testutil.PgCustomerRepo
import testutil.sqliteCache
import testutil.testPgConnection
import testutil.testSQLiteConnection
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
class SyncTest {
  @BeforeEach
  fun setup() {
    testPgConnection().use { con ->
      PgCustomerRepo(con = con, tableName = "customer").recreateTable()
      PgCustomerRepo(con = con, tableName = "customer2").recreateTable()
    }
  }

  @Test
  fun given_no_timestamps_used() {
    testPgConnection().use { con ->
      val srcRepo = PgCustomerRepo(con = con, tableName = "customer")

      val dstRepo = PgCustomerRepo(con = con, tableName = "customer2")
      dstRepo.addUniqueConstraint()

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

        srcRepo.addCustomers(customer1, customer2, customer3)

        val cache = sqliteCache()

        val resultAfterAdd = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
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

        val actual = dstRepo.fetchCustomers()

        assertEquals(expected = customers, actual = actual)

        assertEquals(expected = 0, actual = resultAfterAdd.deleted)
        assertEquals(expected = 3, actual = resultAfterAdd.upserted)

        val updatedCustomer2 = customer2.copy(dateUpdated = LocalDateTime.of(2020, 1, 2, 3, 4, 5), middleInitial = "Z")

        // TEST UPDATE
        srcRepo.updateCustomer(customer = updatedCustomer2)

        val resultAfterUpdate = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
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
        )

        val updatedCustomers = dstRepo.fetchCustomers()

        assertEquals(expected = setOf(customer1, updatedCustomer2, customer3), actual = updatedCustomers)

        assertEquals(expected = 0, actual = resultAfterUpdate.deleted)
        assertEquals(expected = 1, actual = resultAfterUpdate.upserted)

        // TEST DELETE
        srcRepo.deleteCustomer(customerId = 3)

        val resultAfterDelete = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
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
        )

        val customersAfterDelete = dstRepo.fetchCustomers()

        assertEquals(expected = setOf(customer1, updatedCustomer2), actual = customersAfterDelete)

        assertEquals(expected = 1, actual = resultAfterDelete.deleted)
        assertEquals(expected = 0, actual = resultAfterDelete.upserted)
      }
    }
  }

  @Test
  fun given_timestamps_used_and_empty_inital_cache() {
    testPgConnection().use { con ->
      val srcRepo = PgCustomerRepo(con = con, tableName = "customer")

      val dstRepo = PgCustomerRepo(con = con, tableName = "customer2")
      dstRepo.addUniqueConstraint()

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

        srcRepo.addCustomers(customer1, customer2, customer3)

        val cache = sqliteCache()

        val resultAfterAdd = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
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
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val actual = dstRepo.fetchCustomers()

        assertEquals(expected = customers, actual = actual)

        assertEquals(expected = 0, actual = resultAfterAdd.deleted)
        assertEquals(expected = 3, actual = resultAfterAdd.upserted)

        // TEST UPDATE
        val updatedCustomer2 = customer2.copy(
          dateUpdated = LocalDateTime.of(2020, 1, 2, 3, 4, 5),
          middleInitial = "Z"
        )
        srcRepo.updateCustomer(customer = updatedCustomer2)

        val resultAfterUpdate = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
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
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val updatedCustomers = dstRepo.fetchCustomers()

        assertEquals(expected = 3, actual = updatedCustomers.count())

        assertEquals(expected = setOf(customer1, updatedCustomer2, customer3), actual = updatedCustomers)

        assertEquals(expected = 0, actual = resultAfterUpdate.deleted)
        assertEquals(expected = 1, actual = resultAfterUpdate.upserted)

        // TEST DELETE
        srcRepo.deleteCustomer(customerId = 3)

        assertEquals(expected = 2, srcRepo.fetchCustomers().count())

        val resultAfterDelete = sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
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
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val customersAfterDelete = dstRepo.fetchCustomers()

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
      val srcRepo = PgCustomerRepo(con = con, tableName = "customer")

      val dstRepo = PgCustomerRepo(con = con, tableName = "customer2")
      dstRepo.addUniqueConstraint()

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

        srcRepo.addCustomers(customer1, customer2, customer3, customer2dupe)

        val cache = sqliteCache()

        sync(
          srcCon = con,
          dstCon = con,
          cache = cache,
          srcDialect = DbDialect.PostgreSQL,
          dstDialect = DbDialect.PostgreSQL,
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
          timestampFieldNames = setOf("date_added", "date_updated"),
          showSQL = true,
        )

        val actual = dstRepo.fetchCustomers()

        assertEquals(expected = setOf(customer1, customer2dupe, customer3), actual = actual)
      }
    }
  }
}
