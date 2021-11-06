package kda

import kda.domain.DataType
import kda.domain.Dialect
import kda.domain.Field
import kda.domain.Table
import kda.testutil.pgTableExists
import kda.testutil.testPgConnection
import kda.testutil.testSQLiteDbCache
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InspectTableTest {
  @Test
  fun inspectTable_happy_path() {
    testPgConnection().use { con ->
      con.createStatement().use { stmt ->
        stmt.execute("DROP TABLE IF EXISTS sales.customer")
        stmt.execute(
          """
            CREATE TABLE sales.customer (
                customer_id SERIAL PRIMARY KEY
            ,   first_name TEXT
            ,   last_name TEXT
            )
          """.trimIndent()
        )
      }
      assertTrue(pgTableExists(con, schema = "sales", table = "customer2"))

      val actual = inspectTable(
        con = testPgConnection(),
        dialect = Dialect.PostgreSQL,
        schema = "sales",
        table = "customer",
        primaryKeyFieldNames = listOf("customer_id"),
        includeFieldNames = null,
        cache = testSQLiteDbCache(),
      ).getOrThrow()
      val expected = Table(
        schema = "sales",
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int(true)),
          Field(name = "first_name", dataType = DataType.nullableText(null)),
          Field(name = "last_name", dataType = DataType.nullableText(null)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
      assertEquals(expected = expected, actual = actual)
    }
  }
}
