package kda.domain

enum class Operator {
  Equals,
  GreaterThan,
  GreaterThanOrEqualTo,
  LessThan,
  LessThanOrEqualTo;

  override fun toString(): String =
    when (this) {
      Equals               -> "="
      GreaterThan          -> ">"
      GreaterThanOrEqualTo -> ">="
      LessThan             -> "<"
      LessThanOrEqualTo    -> "<="
    }
}
