package kda.adapter.hh

import kda.adapter.std.StdAdapterDetails
import kda.domain.DataType
import kda.domain.DbAdapterDetails
import kda.domain.Field
import kda.domain.Parameter

@ExperimentalStdlibApi
object HHAdapterDetails : DbAdapterDetails {
  private val std = StdAdapterDetails

  override fun fieldDef(field: Field<*>): String = std.fieldDef(field)

  override fun castParameter(dataType: DataType<*>): String = std.castParameter(dataType)

  override fun <T> whereFieldIsEqualTo(field: Field<T>): Set<Parameter> = std.whereFieldIsEqualTo(field)

  override fun <T> whereFieldIsGreaterThan(field: Field<T>): Set<Parameter> = std.whereFieldIsGreaterThan(field)

  override fun <T> whereFieldIsGreaterThanOrEqualTo(field: Field<T>): Set<Parameter> = std.whereFieldIsGreaterThanOrEqualTo(field)

  override fun <T> whereFieldIsLessThan(field: Field<T>): Set<Parameter> = std.whereFieldIsLessThan(field)

  override fun <T> whereFieldIsLessThanOrEqualTo(field: Field<T>): Set<Parameter> = std.whereFieldIsLessThanOrEqualTo(field)

  override fun wrapName(name: String): String = "`$name`"
}
