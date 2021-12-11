package kda.adapter

import kda.domain.BoundParameter
import kda.domain.DbAdapterDetails
import kda.domain.Field
import kda.domain.Parameter
import kda.domain.Row

internal fun Field<*>.toValuesParameter() = Parameter(name = name, dataType = dataType, sql = "?")

internal fun Collection<Field<*>>.toSetValuesEqualToParameters(details: DbAdapterDetails): List<Parameter> =
  sortedBy { it.name }
    .map { field ->
      Parameter(name = field.name, dataType = field.dataType, sql = "${details.wrapName(field.name)} = ?")
    }

internal fun Field<*>.toWhereEqualsParameter(details: DbAdapterDetails): List<Parameter> {
  val wrappedFieldName = details.wrapName(this.name)

  return if (dataType.nullable) {
    listOf(
      Parameter(
        name = name,
        dataType = dataType,
        sql = "$wrappedFieldName = ?"
      ),
      Parameter(
        name = name,
        dataType = dataType,
        sql = "COALESCE($wrappedFieldName, ?) IS NULL"
      ),
    )
  } else {
    listOf(
      Parameter(
        name = name,
        dataType = dataType,
        sql = "$wrappedFieldName = ?"
      ),
    )
  }
}

// internal fun Collection<Field<*>>.toWhereEqualsParameters(details: DbAdapterDetails): List<Parameter> =
//  sortedBy { it.name }.flatMap { it.toWhereEqualsParameter(details = details) }

internal fun Field<*>.toWhereEqualsBoundParameter(details: DbAdapterDetails, row: Row): List<BoundParameter> {
  val wrappedFieldName = details.wrapName(name)

  return if (dataType.nullable) {
    listOf(
      BoundParameter(
        parameter = Parameter(
          name = name,
          dataType = dataType,
          sql = "$wrappedFieldName = ?",
        ),
        value = row.value[this.name],
      ),
      BoundParameter(
        parameter = Parameter(
          name = name,
          dataType = dataType,
          sql = "COALESCE($wrappedFieldName, ?) IS NULL",
        ),
        value = row.value[this.name],
      ),
    )
  } else {
    listOf(
      BoundParameter(
        parameter = Parameter(
          name = name,
          dataType = dataType,
          sql = "$wrappedFieldName = ?",
        ),
        value = row.value[this.name],
      ),
    )
  }
}

internal fun Collection<Field<*>>.toWhereEqualsBoundParameters(details: DbAdapterDetails, row: Row): List<BoundParameter> =
  sortedBy { it.name }
    .flatMap { field ->
      field.toWhereEqualsBoundParameter(details = details, row = row)
    }
