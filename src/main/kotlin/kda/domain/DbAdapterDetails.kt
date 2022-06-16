package kda.domain

interface DbAdapterDetails {

  fun fieldDef(field: Field<*>): String

  fun castParameter(dataType: DataType<*>): String

  fun fullTableName(schema: String?, table: String): String =
    if (schema == null) {
      wrapName(table)
    } else {
      "${wrapName(schema)}.${wrapName(table)}"
    }

  fun <T : Any?> whereFieldIsEqualTo(field: Field<T>): Set<Parameter>
  fun <T : Any?> whereFieldIsGreaterThan(field: Field<T>): Set<Parameter>
  fun <T : Any?> whereFieldIsGreaterThanOrEqualTo(field: Field<T>): Set<Parameter>
  fun <T : Any?> whereFieldIsLessThan(field: Field<T>): Set<Parameter>
  fun <T : Any?> whereFieldIsLessThanOrEqualTo(field: Field<T>): Set<Parameter>

  fun wrapName(name: String): String
}
