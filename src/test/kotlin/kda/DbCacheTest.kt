package kda

import kda.domain.DataType
import kda.domain.Dialect
import kda.domain.Field
import kda.domain.LatestTimestamp
import kda.domain.Table
import kda.testutil.testSQLiteDbConnection
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DbCacheTest {
  @Test
  fun addTableDef_happy_path() {
    testSQLiteDbConnection().use { con ->
      val ds = datasource(con = con, dialect = Dialect.SQLite)

      val cache = DbCache(
        ds = ds,
        showSQL = false,
        maxFloatDigits = 5,
      )

      val tableDef = Table(
        schema = null,
        name = "customer",
        fields = setOf(
          Field("customer_id", DataType.int(true)),
          Field("name", DataType.text(40)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      cache.addTableDef(tableDef)

      val actual = cache.tableDef(schema = null, table = "customer").getOrThrow()

      assertEquals(expected = tableDef, actual = actual)
    }
  }

  @Test
  fun addLatestTimestamp_happy_path() {
    testSQLiteDbConnection().use { con ->
      val ds = datasource(con = con, dialect = Dialect.SQLite)

      val cache = DbCache(
        ds = ds,
        showSQL = true,
        maxFloatDigits = 5,
      )

      val ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5)

      cache.addLatestTimestamp(
        schema = null,
        table = "customer",
        timestamps = setOf(
          LatestTimestamp(
            fieldName = "date_added",
            timestamp = ts,
          )
        )
      ).getOrThrow()

      val actual = cache.latestTimestamps(schema = null, table = "customer").getOrThrow()

      val expected = setOf(LatestTimestamp(fieldName = "date_added", timestamp = ts))

      assertEquals(expected = expected, actual = actual)
    }
  }

  @Test
  fun clearTableDef_happy_path() {
    testSQLiteDbConnection().use { con ->
      val ds = datasource(con = con, dialect = Dialect.SQLite)

      val cache = DbCache(
        ds = ds,
        showSQL = true,
        maxFloatDigits = 5,
      )

      val tableDef = Table(
        schema = null,
        name = "customer",
        fields = setOf(
          Field("customer_id", DataType.int(true)),
          Field("name", DataType.text(40)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )

      cache.addTableDef(tableDef).getOrThrow()

      assertNotNull(cache.tableDef(schema = null, table = "customer").getOrThrow())

      cache.clearTableDef(schema = null, table = "customer").getOrThrow()

      assertNull(cache.tableDef(schema = null, table = "customer").getOrThrow())
    }
  }

  @Test
  fun clearLatestTimestamps_happy_path() {
    testSQLiteDbConnection().use { con ->
      val ds = datasource(con = con, dialect = Dialect.SQLite)

      val cache = DbCache(
        ds = ds,
        showSQL = true,
        maxFloatDigits = 5,
      )

      val ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5)

      val latestTimestamps = setOf(LatestTimestamp(fieldName = "date_added", timestamp = ts))

      cache.addLatestTimestamp(schema = null, table = "customer", timestamps = latestTimestamps)

      assertNotNull(cache.latestTimestamps(schema = null, table = "customer").getOrThrow())

      cache.clearLatestTimestamps(schema = null, table = "customer")

      assertEquals(expected = emptySet(), actual = cache.latestTimestamps(schema = null, table = "customer").getOrThrow())
    }
  }
}
