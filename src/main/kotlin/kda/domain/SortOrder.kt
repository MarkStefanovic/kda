package kda.domain

enum class SortOrder {
  Ascending,
  Descending;

  override fun toString() =
    when (this) {
      Ascending -> "ASC"
      Descending -> "DESC"
    }
}
