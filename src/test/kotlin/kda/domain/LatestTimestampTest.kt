package kda.domain

import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import kotlin.test.assertEquals

class LatestTimestampTest {
  @Test
  fun toPredicate_when_latest_ts_is_null() {
    val latestTimestamp = LatestTimestamp(
      fieldName = "date_added",
      timestamp = null,
    )
    val actual = latestTimestamp.toPredicate()
    val expected = Predicate(
      field = Field(name = "date_added", dataType = NullableLocalDateTimeType),
      value = Value.nullableDatetime(null),
      operator = Operator.GreaterThan,
    )
    assertEquals(expected = expected, actual = actual)
  }

  @Test
  fun toPredicate_when_latest_ts_is_not_null() {
    val ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5)
    val latestTimestamp = LatestTimestamp(
      fieldName = "date_added",
      timestamp = ts,
    )
    val actual = latestTimestamp.toPredicate()
    val expected = Predicate(
      field = Field(name = "date_added", dataType = NullableLocalDateTimeType),
      value = Value.nullableDatetime(ts),
      operator = Operator.GreaterThan,
    )
    assertEquals(expected = expected, actual = actual)
  }
}
