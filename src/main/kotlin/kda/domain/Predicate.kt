package kda.domain

data class Predicate(
  val field: Field,
  val value: Value<*>,
  val operator: Operator,
) {
  val description: String by lazy {
    "${field.name} ${operator.name} ${value.value}"
  }
}
