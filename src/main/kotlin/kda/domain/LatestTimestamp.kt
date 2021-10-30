package kda.domain

import java.time.LocalDateTime

data class LatestTimestamp(
  val fieldName: String,
  val timestamp: LocalDateTime?,
) {
  fun toPredicate() = Predicate(
    field = Field(name = fieldName, dataType = DataType.nullableLocalDateTime),
    operator = Operator.GreaterThan,
    value = Value.nullableDatetime(timestamp),
  )
}
