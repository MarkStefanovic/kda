@file:Suppress("ClassName", "unused")

package kda.domain

data class Field<out T : Any?> (val name: String, val dataType: DataType<T>) {
  @Suppress("UNCHECKED_CAST")
  fun value(row: Map<Field<*>, *>): T = row[this] as T

  override fun toString() = "Field [ name: $name, dataType: ${dataType.description} ]"
}

infix fun <T : Any?> Field<T>.eq(value: T) = BinaryPredicate(parameterName = name, dataType = dataType, operator = Operator.Equals, value = value)
infix fun <T : Any?> Field<T>.ge(value: T) = BinaryPredicate(parameterName = name, dataType = dataType, operator = Operator.GreaterThan, value = value)
infix fun <T : Any?> Field<T>.geq(value: T) = BinaryPredicate(parameterName = name, dataType = dataType, operator = Operator.GreaterThanOrEqualTo, value = value)
infix fun <T : Any?> Field<T>.le(value: T) = BinaryPredicate(parameterName = name, dataType = dataType, operator = Operator.LessThan, value = value)
infix fun <T : Any?> Field<T>.leq(value: T) = BinaryPredicate(parameterName = name, dataType = dataType, operator = Operator.LessThanOrEqualTo, value = value)
