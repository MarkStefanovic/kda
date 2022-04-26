@file:Suppress("SqlResolve")

package kda

import kda.domain.CopyTableResult
import kda.domain.DbDialect
import org.junit.jupiter.api.Test
import testutil.pgTableExists
import testutil.sqliteCache
import testutil.testPgConnection
import testutil.testSQLiteConnection
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
class CopyTableTest {
  @Test
  fun given_dest_does_not_exist_then_it_should_be_created() {
    testPgConnection().use { con: Connection ->
      testSQLiteConnection().use {
        con.createStatement().use { stmt ->
          stmt.execute(
            // language=PostgreSQL
            "DROP TABLE IF EXISTS sales.customer"
          )
          stmt.execute(
            // language=PostgreSQL
            "DROP TABLE IF EXISTS sales.customer2"
          )
          stmt.execute(
            // language=PostgreSQL
            """
            CREATE TABLE sales.customer (
                customer_id SERIAL PRIMARY KEY
            ,   first_name TEXT
            ,   last_name TEXT
            )
            """
          )
        }
        assert(!pgTableExists(con, "sales", "customer2"))

        val cache = sqliteCache()

        val result =
          copyTable(
            srcCon = con,
            dstCon = con,
            cache = cache,
            dstDialect = DbDialect.PostgreSQL,
            srcDbName = "src",
            srcSchema = "sales",
            srcTable = "customer",
            dstDbName = "dst",
            dstSchema = "sales",
            dstTable = "customer2",
            includeFields = setOf("customer_id", "first_name", "last_name"),
            primaryKeyFieldNames = listOf("customer_id"),
            addTimestamp = false,
          )
        assertIs<CopyTableResult>(result)
        assertEquals(
          expected = setOf("customer_id", "first_name", "last_name"),
          actual = result.srcTable.fields.map { fld -> fld.name }.toSet()
        )
        assert(pgTableExists(con, "sales", "customer2"))
      }
    }
  }

  @Test
  fun given_no_primary_is_enforced_at_db_level() {
    testSQLiteConnection().use { con: Connection ->
      con.createStatement().use { stmt ->
        stmt.execute(
          // language=SQLite
          "DROP TABLE IF EXISTS customer"
        )

        stmt.execute(
          // language=SQLite
          "DROP TABLE IF EXISTS customer2"
        )

        stmt.execute(
          // language=SQLite
          """
          CREATE TABLE customer (
              customer_id INTEGER NOT NULL
          ,   first_name TEXT
          ,   last_name TEXT
          )
          """
        )
      }

      val cache = sqliteCache()

      val result =
        copyTable(
          srcCon = con,
          dstCon = con,
          cache = cache,
          dstDialect = DbDialect.SQLite,
          srcDbName = "src",
          srcSchema = null,
          srcTable = "customer",
          dstDbName = "dst",
          dstSchema = null,
          dstTable = "customer2",
          includeFields = setOf("customer_id", "first_name", "last_name"),
          primaryKeyFieldNames = listOf("customer_id"),
        )

      assertIs<CopyTableResult>(result)

      assertEquals(
        expected = setOf("customer_id", "first_name", "last_name"),
        actual = result.srcTable.fields.map { fld -> fld.name }.toSet()
      )
    }
  }
}
