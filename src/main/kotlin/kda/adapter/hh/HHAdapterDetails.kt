package kda.adapter.hh

import kda.adapter.std.StdAdapterDetails
import kda.domain.DataType
import kda.domain.DbAdapterDetails
import kda.domain.Field

@ExperimentalStdlibApi
object HHAdapterDetails : DbAdapterDetails {
  private val std = StdAdapterDetails

  override fun castParameter(dataType: DataType<*>): String =
    std.castParameter(dataType = dataType)

  override fun fieldDef(field: Field<*>): String =
    std.fieldDef(field = field)

  override fun <T : Any?> whereFieldIsEqualTo(field: Field<T>) =
    std.whereFieldIsEqualTo(field = field)

  override fun <T : Any?> whereFieldIsGreaterThan(field: Field<T>) =
    std.whereFieldIsGreaterThan(field = field)

  override fun <T : Any?> whereFieldIsGreaterThanOrEqualTo(field: Field<T>) =
    std.whereFieldIsGreaterThanOrEqualTo(field = field)

  override fun <T : Any?> whereFieldIsLessThan(field: Field<T>) =
    std.whereFieldIsLessThan(field = field)

  override fun <T : Any?> whereFieldIsLessThanOrEqualTo(field: Field<T>) =
    std.whereFieldIsLessThanOrEqualTo(field = field)

  override fun wrapName(name: String): String = "`$name`"
}
