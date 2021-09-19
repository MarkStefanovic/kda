package kda

import kda.domain.Dialect
import kda.domain.Field
import kda.domain.IntType
import kda.domain.NullableStringType
import kda.domain.Table
import kda.shared.tableExists
import kda.shared.testDbCache
import kda.shared.testPgConnection
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
      assertTrue(tableExists(con, schema = "sales", table = "customer2"))

      val actual = inspectTable(
        con = testPgConnection(),
        dialect = Dialect.PostgreSQL,
        schema = "sales",
        table = "customer",
        primaryKeyFieldNames = listOf("customer_id"),
        includeFieldNames = null,
        cache = testDbCache(),
      ).getOrThrow()
      val expected = Table(
        schema = "sales",
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = IntType(true)),
          Field(name = "first_name", dataType = NullableStringType(null)),
          Field(name = "last_name", dataType = NullableStringType(null)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
      assertEquals(expected = expected, actual = actual)
    }
  }
}
