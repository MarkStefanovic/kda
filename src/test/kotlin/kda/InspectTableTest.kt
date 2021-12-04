package kda

import kda.domain.DataType
import kda.domain.DbDialect
import kda.domain.Field
import kda.domain.Table
import kda.testutil.testSQLiteConnection
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class InspectTableTest {
  @Test
  fun inspectTable_happy_path() {
    testSQLiteConnection().use { con ->
      con.createStatement().use { stmt ->
        stmt.execute("DROP TABLE IF EXISTS customer")
        stmt.execute(
          """
            |CREATE TABLE customer (
            |    customer_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
            |,   first_name TEXT NULL
            |,   last_name TEXT NULL
            |)
          """.trimMargin()
        )
      }

      val actual = inspectTable(
        con = con,
        cacheCon = con,
        cacheDialect = DbDialect.SQLite,
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
