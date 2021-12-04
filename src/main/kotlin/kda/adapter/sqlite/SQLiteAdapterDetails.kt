package kda.adapter.sqlite

import kda.adapter.std.StdAdapterDetails
import kda.domain.DataType
import kda.domain.DbAdapterDetails
import kda.domain.Field
import kda.domain.Parameter

@ExperimentalStdlibApi
object SQLiteAdapterDetails : DbAdapterDetails {
  private val std = StdAdapterDetails

  override fun castParameter(dataType: DataType<*>): String =
    std.castParameter(dataType = dataType)

  override fun fieldDef(field: Field<*>): String =
    std.fieldDef(field = field)

  override fun <T : Any?> whereFieldIsEqualTo(field: Field<T>): Set<Parameter> =
    std.whereFieldIsEqualTo(field = field)

  override fun <T : Any?> whereFieldIsGreaterThan(field: Field<T>): Set<Parameter> =
    std.whereFieldIsGreaterThan(field = field)

  override fun <T : Any?> whereFieldIsGreaterThanOrEqualTo(field: Field<T>): Set<Parameter> =
    std.whereFieldIsGreaterThanOrEqualTo(field = field)

  override fun <T : Any?> whereFieldIsLessThan(field: Field<T>): Set<Parameter> =
    std.whereFieldIsLessThan(field = field)

  override fun <T : Any?> whereFieldIsLessThanOrEqualTo(field: Field<T>): Set<Parameter> =
    std.whereFieldIsLessThanOrEqualTo(field = field)

  override fun wrapName(name: String): String =
    std.wrapName(name = name)
}
