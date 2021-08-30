package kda.domain

data class Predicate(
  val field: Field,
  val value: Value<*>,
  val operator: Operator,
)
