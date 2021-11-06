package kda

import kda.domain.CopyTableResult
import kda.domain.Dialect
import kda.testutil.DummyCache
import kda.testutil.pgTableExists
import kda.testutil.testPgConnection
import kda.testutil.testSQLiteDbConnection
import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertIs

class CopyTableTest {
  @Test
  fun given_dest_does_not_exist_then_it_should_be_created() {
    testPgConnection().use { srcCon: Connection ->
      testPgConnection().use { destCon: Connection ->
        destCon.createStatement().use { stmt ->
          stmt.execute("DROP TABLE IF EXISTS sales.customer")
          stmt.execute("DROP TABLE IF EXISTS sales.customer2")
          stmt.execute(
            """
            CREATE TABLE sales.customer (
                customer_id SERIAL PRIMARY KEY
            ,   first_name TEXT
            ,   last_name TEXT
            )
            """
          )
        }
        assert(!pgTableExists(destCon, "sales", "customer2"))

        val result =
          copyTable(
            srcCon = srcCon,
            destCon = destCon,
            srcDialect = Dialect.PostgreSQL,
            destDialect = Dialect.PostgreSQL,
            srcSchema = "sales",
            srcTable = "customer",
            destSchema = "sales",
            destTable = "customer2",
            includeFields = setOf("customer_id", "first_name", "last_name"),
            primaryKeyFields = listOf("customer_id"),
          ).getOrThrow()
        assertIs<CopyTableResult>(result)
        assertEquals(
          expected = setOf("customer_id", "first_name", "last_name"),
          actual = result.srcTableDef.fields.map { fld -> fld.name }.toSet()
        )
        assert(pgTableExists(destCon, "sales", "customer2"))
      }
    }
  }

  @Test
  fun given_no_primary_is_enforced_at_db_level() {
    testSQLiteDbConnection().use { con: Connection ->
      con.createStatement().use { stmt ->
        stmt.execute("DROP TABLE IF EXISTS customer")

        stmt.execute("DROP TABLE IF EXISTS customer2")

        stmt.execute(
          """
          CREATE TABLE customer (
              customer_id INTEGER NOT NULL
          ,   first_name TEXT
          ,   last_name TEXT
          )
          """
        )
      }

      val result =
        copyTable(
          srcCon = con,
          destCon = con,
          srcDialect = Dialect.SQLite,
          destDialect = Dialect.SQLite,
          srcSchema = null,
          srcTable = "customer",
          destSchema = null,
          destTable = "customer2",
          includeFields = setOf("customer_id", "first_name", "last_name"),
          primaryKeyFields = listOf("customer_id"),
          cache = DummyCache(),
        ).getOrThrow()

      assertIs<CopyTableResult>(result)

      assertEquals(
        expected = setOf("customer_id", "first_name", "last_name"),
        actual = result.srcTableDef.fields.map { fld -> fld.name }.toSet()
      )
    }
  }
}
