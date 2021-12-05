package kda.domain

data class Predicate <out T : Any?>(
  val field: Field<T>,
  val operator: Operator,
  val value: T,
) {
  val description: String by lazy {
    val op = when (operator) {
      Operator.Equals -> "="
      Operator.GreaterThan -> ">"
      Operator.GreaterThanOrEqualTo -> ">="
      Operator.LessThan -> "<"
      Operator.LessThanOrEqualTo -> "<="
    }
    "${field.name} $op $value"
  }

  fun toBoundParameters(details: DbAdapterDetails): Set<BoundParameter> {
    val parameters: Set<Parameter> = when (operator) {
      Operator.Equals -> details.whereFieldIsEqualTo(field = field)
      Operator.GreaterThan -> details.whereFieldIsGreaterThan(field = field)
      Operator.GreaterThanOrEqualTo -> details.whereFieldIsGreaterThanOrEqualTo(field = field)
      Operator.LessThan -> details.whereFieldIsLessThan(field = field)
      Operator.LessThanOrEqualTo -> details.whereFieldIsLessThanOrEqualTo(field = field)
    }
    return parameters.map { BoundParameter(parameter = it, value = value) }.toSet()
  }

  override fun toString(): String =
    """
      |Predicate [
      |  field: $field
      |  operator: $operator
      |  value: $value 
      |]
    """.trimMargin()
}
