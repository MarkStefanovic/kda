package kda.domain

@JvmInline
value class Criteria(val orClause: Set<Set<Predicate>>) {
  val description: String
    get() =
      orClause
        .joinToString(" or ") { andClause ->
          andClause
            .sortedBy { it.description }
            .joinToString(" and ") { "(${it.description})" }
        }
}
