package kda.domain

data class BinaryPredicate <out T : Any?>(
  val parameterName: String,
  val dataType: DataType<T>,
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
    "$parameterName $op $value"
  }

  fun toBoundParameters(details: DbAdapterDetails): Set<BoundParameter> {
    val field = Field(name = parameterName, dataType = dataType)

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
      |BinaryPredicate [
      |  parameterName: $parameterName
      |  dataType: $dataType
      |  operator: $operator
      |  value: $value 
      |]
    """.trimMargin()
}
