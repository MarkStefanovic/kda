package kda.adapter

import kda.adapter.pg.PgCache
import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import org.junit.jupiter.api.Test
import testutil.testPgConnection
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class PgCacheTest {
  @Test
  fun addTableDef_happy_path() {
    testPgConnection().use { con ->
      con.createStatement().use { statement ->
        //language=PostgreSQL
        statement.execute(
          """
          |-- noinspection SqlResolve @ any/"ketl"
          |DROP TABLE IF EXISTS ketl.table_def
          """.trimMargin()
        )
      }

      val cache = PgCache(
        connector = { testPgConnection() },
        cacheSchema = "ketl",
        showSQL = false,
      )

      val expected = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "name", dataType = DataType.nullableText(null)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      cache.addTable(dbName = "db", schema = null, table = expected)

      val actual = cache.getTable(dbName = "db", schema = null, table = "customer")

      assertNotNull(actual)

      assertEquals(expected = expected.name, actual = actual.name)

      assertEquals(expected = expected.primaryKeyFieldNames, actual = actual.primaryKeyFieldNames)

      assertEquals(expected = expected.fields, actual = actual.fields)
    }
  }
}
