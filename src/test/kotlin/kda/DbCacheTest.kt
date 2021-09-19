package kda

import kda.domain.Field
import kda.domain.IntType
import kda.domain.LatestTimestamp
import kda.domain.StringType
import kda.domain.Table
import kda.shared.testDbCache
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DbCacheTest {
  @Test
  fun addTableDef_happy_path() {
    val cache = testDbCache()

    val tableDef = Table(
      schema = null,
      name = "customer",
      fields = setOf(
        Field("customer_id", IntType(true)),
        Field("name", StringType(40)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    cache.addTableDef(tableDef)
    val actual = cache.tableDef(schema = null, table = "customer").getOrThrow()
    assertEquals(expected = tableDef, actual = actual)
  }

  @Test
  fun addLatestTimestamp_happy_path() {
    val cache = testDbCache()
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
    )
    val actual = cache.latestTimestamps(schema = null, table = "customer").getOrThrow()
    val expected = setOf(LatestTimestamp(fieldName = "date_added", timestamp = ts))
    assertEquals(expected = expected, actual = actual)
  }

  @Test
  fun clearTableDef_happy_path() {
    val cache = testDbCache()
    val tableDef = Table(
      schema = null,
      name = "customer",
      fields = setOf(
        Field("customer_id", IntType(true)),
        Field("name", StringType(40)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    cache.addTableDef(tableDef)
    assertNotNull(cache.tableDef(schema = null, table = "customer").getOrThrow())
    cache.clearTableDef(schema = null, table = "customer")
    assertNull(cache.tableDef(schema = null, table = "customer").getOrThrow())
  }

  @Test
  fun clearLatestTimestamps_happy_path() {
    val cache = testDbCache()
    val ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5)
    val latestTimestamps = setOf(LatestTimestamp(fieldName = "date_added", timestamp = ts))
    cache.addLatestTimestamp(schema = null, table = "customer", timestamps = latestTimestamps)
    assertNotNull(cache.latestTimestamps(schema = null, table = "customer").getOrThrow())
    cache.clearLatestTimestamps(schema = null, table = "customer")
    assertEquals(expected = emptySet(), actual = cache.latestTimestamps(schema = null, table = "customer").getOrThrow())
  }
}
