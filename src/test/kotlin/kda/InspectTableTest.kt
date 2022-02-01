@file:Suppress("SqlResolve")

package kda

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import org.junit.jupiter.api.Test
import testutil.sqliteCache
import testutil.testSQLiteConnection
import kotlin.test.assertEquals

class InspectTableTest {

  @Test
  fun sqlite_inspectTable_happy_path() {
    testSQLiteConnection().use { con ->
      con.createStatement().use { stmt ->
        stmt.execute(
          // language=SQLite
          "DROP TABLE IF EXISTS customer"
        )
        stmt.execute(
          // language=SQLite
          """
            |CREATE TABLE customer (
            |    customer_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
            |,   first_name TEXT NULL
            |,   last_name TEXT NULL
            |)
          """.trimMargin()
        )
      }

      val cache = sqliteCache()

      val actual = inspectTable(
        con = con,
        cache = cache,
        dbName = "db",
        schema = null,
        table = "customer",
        primaryKeyFieldNames = listOf("customer_id"),
        includeFieldNames = null,
      )

      val expected = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "first_name", dataType = DataType.nullableText(null)),
          Field(name = "last_name", dataType = DataType.nullableText(null)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
      assertEquals(expected = expected, actual = actual)
    }
  }
}
