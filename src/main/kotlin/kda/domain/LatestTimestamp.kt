package kda.domain

import java.time.LocalDateTime

data class LatestTimestamp(
  val fieldName: String,
  val timestamp: LocalDateTime?,
) {
  fun toPredicate() = Predicate(
    field = Field(name = fieldName, dataType = LocalDateTimeType),
    operator = Operator.GreaterThan,
    value = NullableLocalDateTimeValue(timestamp),
  )
}
