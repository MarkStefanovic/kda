@file:OptIn(ExperimentalStdlibApi::class)

package kda

import kda.domain.CopyTableResult
import kda.domain.DbDialect
import kda.testutil.pgTableExists
import kda.testutil.testPgConnection
import kda.testutil.testSQLiteConnection
import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertIs

class CopyTableTest {
  @Test
  fun given_dest_does_not_exist_then_it_should_be_created() {
    testPgConnection().use { con: Connection ->
      testSQLiteConnection().use { cacheCon: Connection ->
        con.createStatement().use { stmt ->
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
        assert(!pgTableExists(con, "sales", "customer2"))

        val result =
          copyTable(
            srcCon = con,
            dstCon = con,
            cacheCon = cacheCon,
            dstDialect = DbDialect.PostgreSQL,
            cacheDialect = DbDialect.SQLite,
            srcSchema = "sales",
            srcTable = "customer",
            dstSchema = "sales",
            dstTable = "customer2",
            includeFields = setOf("customer_id", "first_name", "last_name"),
            primaryKeyFieldNames = listOf("customer_id"),
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
          dstCon = con,
          cacheCon = con,
          dstDialect = DbDialect.SQLite,
          cacheDialect = DbDialect.SQLite,
          srcSchema = null,
          srcTable = "customer",
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
