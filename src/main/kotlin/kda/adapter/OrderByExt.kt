package kda.adapter

import kda.domain.DbAdapterDetails
import kda.domain.OrderBy
import kda.domain.SortOrder

fun List<OrderBy>.toSQL(details: DbAdapterDetails): String =
  joinToString(", ") { orderBy ->
    val direction = when (orderBy.order) {
      SortOrder.Ascending -> "ASC"
      SortOrder.Descending -> "DESC"
    }
    "${details.wrapName(orderBy.field.name)} $direction"
  }
