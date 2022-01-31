package kda.adapter.std

import kda.domain.DataType
import kda.domain.DbAdapterDetails
import kda.domain.Field
import kda.domain.Parameter

@ExperimentalStdlibApi
object StdAdapterDetails : DbAdapterDetails {
  override fun fieldDef(field: Field<*>): String {
    val wrappedName = wrapName(field.name)

    val nullability = if (field.dataType.nullable) "NULL" else "NOT NULL"

    return when (field.dataType) {
      DataType.bigInt, DataType.nullableBigInt -> "$wrappedName BIGINT $nullability"
      DataType.bool, DataType.nullableBool -> "$wrappedName BOOLEAN $nullability"
      is DataType.decimal -> "$wrappedName NUMERIC(${field.dataType.precision}, ${field.dataType.scale}) $nullability"
      is DataType.nullableDecimal -> "$wrappedName NUMERIC(${field.dataType.precision}, ${field.dataType.scale}) $nullability"
      DataType.float, DataType.nullableFloat -> "$wrappedName FLOAT $nullability"
      DataType.int, DataType.nullableInt -> "$wrappedName INT $nullability"
      DataType.localDate, DataType.nullableLocalDate -> "$wrappedName DATE $nullability"
      DataType.localDateTime, DataType.nullableLocalDateTime -> "$wrappedName TIMESTAMP $nullability"
      is DataType.nullableText -> if (field.dataType.maxLength == null) "$wrappedName TEXT $nullability" else "$wrappedName VARCHAR(${field.dataType.maxLength}) $nullability"
      is DataType.text -> if (field.dataType.maxLength == null) "$wrappedName TEXT $nullability" else "$wrappedName VARCHAR(${field.dataType.maxLength}) $nullability"
    }
  }

  override fun castParameter(dataType: DataType<*>): String = when (dataType) {
    DataType.bigInt, DataType.nullableBigInt -> "CAST(? AS BIGINT)"
    DataType.bool, DataType.nullableBool -> "CAST(? AS BOOLEAN)"
    is DataType.decimal -> "CAST(? AS NUMERIC(${dataType.precision}, ${dataType.scale})"
    is DataType.nullableDecimal -> "CAST(? AS NUMERIC(${dataType.precision}, ${dataType.scale})"
    DataType.float, DataType.nullableFloat -> "CAST(? AS FLOAT)"
    DataType.int, DataType.nullableInt -> "CAST(? AS INT)"
    DataType.localDate, DataType.nullableLocalDate -> "CAST(? AS DATE)"
    DataType.localDateTime, DataType.nullableLocalDateTime -> "CAST(? AS TIMESTAMP)"
    is DataType.nullableText -> if (dataType.maxLength == null) "CAST(? AS TEXT)" else "CAST(? AS VARCHAR(${dataType.maxLength})"
    is DataType.text -> if (dataType.maxLength == null) "CAST(? AS TEXT)" else "CAST(? AS VARCHAR(${dataType.maxLength})"
  }

  override fun <T : Any?> whereFieldIsEqualTo(field: Field<T>): Set<Parameter> {
    val wrappedFieldName = wrapName(name = field.name)

    return if (field.dataType.nullable) {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName = ?"),
        Parameter(name = field.name, dataType = field.dataType, sql = "COALESCE($wrappedFieldName, ?) IS NULL"),
      )
    } else {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName = ?"),
      )
    }
  }

  override fun <T : Any?> whereFieldIsGreaterThan(field: Field<T>): Set<Parameter> {
    val wrappedFieldName = wrapName(name = field.name)

    return if (field.dataType.nullable) {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName > ?"),
        Parameter(
          name = field.name,
          dataType = field.dataType,
          sql = "$wrappedFieldName IS NOT NULL AND ${castParameter(dataType = field.dataType)} IS NULL"
        ),
      )
    } else {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName > ?"),
      )
    }
  }

  override fun <T : Any?> whereFieldIsGreaterThanOrEqualTo(field: Field<T>): Set<Parameter> {
    val wrappedFieldName = wrapName(name = field.name)

    return if (field.dataType.nullable) {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName >= ?"),
        Parameter(
          name = field.name,
          dataType = field.dataType,
          sql = "${castParameter(dataType = field.dataType)} IS NULL"
        ),
      )
    } else {
      setOf(Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName >= ?"))
    }
  }

  override fun <T : Any?> whereFieldIsLessThan(field: Field<T>): Set<Parameter> {
    val wrappedFieldName = wrapName(name = field.name)

    return if (field.dataType.nullable) {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName < ?"),
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName IS NULL AND ${castParameter(dataType = field.dataType)} IS NOT NULL"),
      )
    } else {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName <= ?"),
      )
    }
  }

  override fun <T : Any?> whereFieldIsLessThanOrEqualTo(field: Field<T>): Set<Parameter> {
    val wrappedFieldName = wrapName(name = field.name)

    return if (field.dataType.nullable) {
      setOf(
        Parameter(
          name = field.name,
          dataType = field.dataType,
          sql = "$wrappedFieldName <= ? OR ${castParameter(dataType = field.dataType)} IS NULL"
        ),
      )
    } else {
      setOf(
        Parameter(name = field.name, dataType = field.dataType, sql = "$wrappedFieldName <= ?"),
      )
    }
  }

  override fun wrapName(name: String) = "\"$name\""
}
