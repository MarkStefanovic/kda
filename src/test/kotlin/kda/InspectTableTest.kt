@file:Suppress("SqlResolve")

package kda

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import org.junit.jupiter.api.Test
import testutil.sqliteCache
import testutil.testPgConnection
import testutil.testSQLiteConnection
import kotlin.test.assertEquals

class InspectTableTest {

  @Test
  fun sqlite_inspectTable_happy_path() {
    testPgConnection().use { con ->
      con.createStatement().use { statement ->
        statement.execute(
          //language=PostgreSQL
          "DROP TABLE IF EXISTS sales.customer"
        )
        statement.execute(
          // language=PostgreSQL
          """
          CREATE TABLE sales.customer (
              customer_id INT NOT NULL
          ,   first_name TEXT NOT NULL
          ,   last_name TEXT NOT NULL
          ,   middle_initial TEXT NULL
          ,   first_login TIMESTAMP(0) NOT NULL
          ,   last_login TIMESTAMP(0) NULL
          ,   date_added TIMESTAMPTZ(0) NOT NULL DEFAULT now()
          ,   date_updated TIMESTAMPTZ(0) NULL
          )
          """
        )
      }

      testSQLiteConnection().use {
        val cache = sqliteCache()

        val actual = inspectTable(
          con = con,
          cache = cache,
          dbName = "postgres",
          schema = "sales",
          table = "customer",
          primaryKeyFieldNames = listOf("customer_id"),
          includeFieldNames = null,
        )

        val expected = Table(
          name = "customer",
          fields = setOf(
            Field(name = "customer_id", dataType = DataType.int),
            Field(name = "first_name", dataType = DataType.text(null)),
            Field(name = "last_name", dataType = DataType.text(null)),
            Field(name = "middle_initial", dataType = DataType.nullableText(null)),
            Field(name = "first_login", dataType = DataType.timestamp(0)),
            Field(name = "last_login", dataType = DataType.nullableTimestamp(0)),
            Field(name = "date_added", dataType = DataType.timestampUTC(0)),
            Field(name = "date_updated", dataType = DataType.nullableTimestampUTC(0)),
          ),
          primaryKeyFieldNames = listOf("customer_id"),
        )
        assertEquals(expected = expected, actual = actual)
      }
    }
  }
}
