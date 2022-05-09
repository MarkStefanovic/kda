@file:Suppress("SqlResolve")

package testutil

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PgCustomerRepoTest {
  @Test
  fun tableExists() {
    testPgConnection().use { con ->
      con.createStatement().use { statement ->
        statement.execute(
          // language=PostgreSQL
          "DROP TABLE IF EXISTS sales.customer"
        )
      }
      assertFalse(pgTableExists(con, schema = "sales", table = "customer"))

      PgCustomerRepo(con = con).recreateTable()
      assertTrue(pgTableExists(con, schema = "sales", table = "customer"))
    }
  }
}
