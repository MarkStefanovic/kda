package kda.domain

data class OrderBy(val field: Field<*>, val order: SortOrder) {
  override fun toString() = "${field.name} $order"
}
