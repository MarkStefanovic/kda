package kda.adapter

import kda.adapter.sqlite.SQLiteCache
import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import org.junit.jupiter.api.Test
import testutil.testSQLiteConnection
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SQLiteCacheTest {
  @Test
  fun addTableDef_happy_path() {
    testSQLiteConnection().use { con ->
      val cache = SQLiteCache(connector = { con }, showSQL = false)

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
