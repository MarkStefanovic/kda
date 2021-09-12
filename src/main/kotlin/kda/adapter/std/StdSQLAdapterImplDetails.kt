package kda.adapter.std

import kda.domain.*
import java.math.BigDecimal
import java.math.RoundingMode
import java.text.DecimalFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

open class StdSQLAdapterImplDetails : SQLAdapterImplDetails {
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  override fun fullTableName(schema: String?, table: String): String =
    if (schema == null) {
      wrapName(table)
    } else {
      "${wrapName(schema)}.${wrapName(table)}"
    }

  override fun renderCriteria(criteria: Set<Criteria>): String =
    criteria.sortedBy { it.description }.joinToString(" OR ") { c ->
      c.predicates.joinToString(" AND ") { predicate: Predicate ->
        val operator =
          when (predicate.operator) {
            Operator.Equals -> " = "
            Operator.GreaterThan -> " > "
            Operator.LessThan -> " < "
            Operator.GreaterThanOrEqualTo -> " >= "
            Operator.LessThanOrEqualTo -> " <= "
          }
        "(${wrapName(predicate.field.name)} $operator ${wrapValue(predicate.value)})"
      }
    }

  override fun wrapName(name: String) = "\"${name.lowercase()}\""

  override fun wrapValue(value: Value<*>): String =
    when (value) {
      is BoolValue -> wrapBoolValue(value.value)
      is DecimalValue -> wrapDecimalValue(value.value, precision = value.precision, scale = value.scale)
      is FloatValue -> wrapFloatValue(value.value, maxDigits = value.maxDigits)
      is IntValue -> wrapIntValue(value.value)
      is LocalDateValue -> wrapLocalDateValue(value.value)
      is LocalDateTimeValue -> wrapLocalDateTimeValue(value.value)
      is StringValue -> wrapStringValue(value.value, maxLength = value.maxLength)
      is NullableBoolValue -> wrapBoolValue(value.value)
      is NullableDecimalValue -> wrapDecimalValue(value.value, precision = value.precision, scale = value.scale)
      is NullableFloatValue -> wrapFloatValue(value.value, maxDigits = value.maxDigits)
      is NullableIntValue -> wrapIntValue(value.value)
      is NullableLocalDateValue -> wrapLocalDateValue(value.value)
      is NullableLocalDateTimeValue -> wrapLocalDateTimeValue(value.value)
      is NullableStringValue -> wrapStringValue(value = value.value, maxLength = value.maxLength)
    }

  override fun wrapBoolValue(value: Boolean?): String =
    when {
      value == null -> "CAST(NULL AS INT)"
      value -> "1"
      else -> "0"
    }

  override fun wrapDecimalValue(value: BigDecimal?, precision: Int, scale: Int): String =
    if (value == null) "CAST(NULL AS DECIMAL($precision, $scale))"
    else
      with(DecimalFormat("#.#")) {
        roundingMode = RoundingMode.CEILING
        maximumFractionDigits = scale
        format(value)
      }

  override fun wrapFloatValue(value: Float?, maxDigits: Int): String =
    if (value == null) "CAST(NULL AS FLOAT)" else "%.${maxDigits}f".format(value)

  override fun wrapIntValue(value: Int?): String = value?.toString() ?: "CAST(NULL AS BIGINT)"

  override fun wrapLocalDateValue(value: LocalDate?): String =
    if (value == null) "CAST(NULL AS DATE)" else "DATE '${value.format(dateFormatter)}'"

  override fun wrapLocalDateTimeValue(value: LocalDateTime?): String =
    if (value == null) "CAST(NULL AS TIMESTAMP)" else "TIMESTAMP '${value.format(dateTimeFormatter)}'"

  override fun wrapStringValue(value: String?, maxLength: Int?): String =
    if (value == null) {
      "NULL"
    } else {
      val escaped = (if (maxLength == null) value else value.substring(0, value.length)).replace("'", "''")
      "'$escaped'"
    }

  override fun fieldDef(field: Field): String {
    val wrappedFieldName = wrapName(field.name)
    val dataType =
      when (field.dataType) {
        BoolType -> "BOOL NOT NULL"
        is DecimalType -> "DECIMAL(${field.dataType.precision}, ${field.dataType.scale}) NOT NULL"
        is FloatType -> "FLOAT NOT NULL"
        is IntType -> "INT NOT NULL"
        LocalDateTimeType -> "TIMESTAMP NOT NULL"
        LocalDateType -> "DATE NOT NULL"
        is StringType -> "TEXT NULL"
        NullableBoolType -> "BOOL NULL"
        is NullableDecimalType ->
          "DECIMAL(${field.dataType.precision}, ${field.dataType.scale}) NULL"
        is NullableFloatType -> "FLOAT NULL"
        NullableLocalDateTimeType -> "TIMESTAMP NULL"
        NullableLocalDateType -> "DATE NULL"
        is NullableIntType -> "INT NULL"
        is NullableStringType -> "TEXT NULL"
      }
    return "$wrappedFieldName $dataType"
  }

  override fun maxValue(fieldName: String): String {
    val fldName = wrapName(fieldName)
    return "MAX($fldName) AS $fldName"
  }

  override fun valuesExpression(fieldNames: List<String>, rows: Set<Row>, tableAlias: String?): String {
    val sortedFieldNames = fieldNames.sorted()
    return rows.joinToString(", ") { row ->
      rowValuesExpression(sortedFieldNames = sortedFieldNames, row = row, tableAlias = tableAlias)
    }
  }

  override fun joinFields(table: Table, leftTableAlias: String, rightTableAlias: String) =
    fieldsEqual(
      fieldNames = table.primaryKeyFieldNames.toSet(),
      sep = " AND ",
      rightTableAlias = rightTableAlias,
      leftTableAlias = leftTableAlias,
    )

  override fun fieldNameCSV(fieldNames: Set<String>, tableAlias: String?) =
    fieldNames.sorted().joinToString(", ") { fieldName ->
      if (tableAlias == null) {
        wrapName(fieldName)
      } else {
        "$tableAlias.${wrapName(fieldName)}"
      }
    }

  override fun setValues(table: Table, rightTableAlias: String) =
    fieldsEqual(
      fieldNames = table.sortedFieldNames.toSet() - table.primaryKeyFieldNames.toSet(),
      sep = ", ",
      rightTableAlias = rightTableAlias,
      leftTableAlias = null,
    )

  private fun fieldsEqual(
    fieldNames: Set<String>,
    sep: String = " AND ",
    rightTableAlias: String,
    leftTableAlias: String? = null,
  ): String =
    fieldNames
      .sorted()
      .joinToString(sep) { fld ->
        if (leftTableAlias == null) {
          "${wrapName(fld)} = $rightTableAlias.${wrapName(fld)}"
        } else {
          "$leftTableAlias.${wrapName(fld)} = $rightTableAlias.${wrapName(fld)}"
        }
      }

  private fun rowValuesExpression(sortedFieldNames: List<String>, row: Row, tableAlias: String? = null): String {
    val valueCSV = sortedFieldNames.joinToString(", ") { fldName ->
      if (tableAlias == null) {
        wrapValue(row.value(fldName))
      } else {
        "$tableAlias.${wrapValue(row.value(fldName))}"
      }
    }
    return "($valueCSV)"
  }
}
