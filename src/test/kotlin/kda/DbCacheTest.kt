package kda

import kda.adapter.sqlite.SQLiteCache
import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import org.junit.jupiter.api.Test
import testutil.testSQLiteConnection
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class DbCacheTest {
  @Test
  fun addTableDef_happy_path() {
    testSQLiteConnection().use { con ->
      val cache = SQLiteCache(con = con, showSQL = false)

      val expected = Table(
        name = "customer",
        fields = setOf(
          Field(name = "customer_id", dataType = DataType.int),
          Field(name = "name", dataType = DataType.nullableText(null)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      cache.addTable(schema = null, table = expected)

      val actual = cache.getTable(schema = null, table = "customer")

      assertNotNull(actual)

      assertEquals(expected = expected.name, actual = actual.name)

      assertEquals(expected = expected.primaryKeyFieldNames, actual = actual.primaryKeyFieldNames)

      assertEquals(expected = expected.fields, actual = actual.fields)
    }
  }

  @Test
  fun addLatestTimestamp_happy_path() {
    testSQLiteConnection().use { con ->
      val cache = SQLiteCache(con = con, showSQL = false)

      val ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5)

      cache.addTimestamp(
        schema = null,
        table = "customer",
        fieldName = "date_added",
        ts = ts,
      )

      val actual = cache.getTimestamp(schema = null, table = "customer", fieldName = "date_added")

      assertEquals(expected = ts, actual = actual)
    }
  }
}
