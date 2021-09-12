package kda.domain

@JvmInline
value class Criteria(val predicates: Set<Predicate>) {
  val description: String
    get() = predicates.sortedBy { it.description }.joinToString(" and ") { it.description }
}
