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

      val table = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "name", dataType = DataType.nullableText(null)),
          Field(name = "date_added", dataType = DataType.timestamp(6)),
          Field(name = "date_updated", dataType = DataType.nullableTimestamp(6)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      cache.addTable(dbName = "db", schema = null, table = table)

      val actual = cache.getTable(dbName = "db", schema = null, table = "customer")

      assertNotNull(actual)

      assertEquals(expected = table.name, actual = actual.name)

      assertEquals(expected = table.primaryKeyFieldNames, actual = actual.primaryKeyFieldNames)

      assertEquals(expected = table.fields, actual = actual.fields)
    }
  }
}
