package kda.adapter.std

import kda.domain.Criteria
import kda.domain.DataType
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Operator
import kda.domain.Predicate
import kda.domain.Row
import kda.domain.SQLAdapterImplDetails
import kda.domain.Table
import kda.domain.Value
import java.math.BigDecimal
import java.math.RoundingMode
import java.text.DecimalFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

open class StdSQLAdapterImplDetails : SQLAdapterImplDetails {
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")

  override fun fullTableName(schema: String?, table: String): String =
    if (schema == null) {
      wrapName(table)
    } else {
      "${wrapName(schema)}.${wrapName(table)}"
    }

  override fun renderCriteria(criteria: Set<Criteria>, tableAlias: String?): String? {
    val prefix = if (tableAlias == null) {
      ""
    } else {
      "$tableAlias."
    }
    val orClause = criteria.sortedBy { it.description }.mapNotNull { c ->
      val andClause = c.predicates.mapNotNull { predicate: Predicate ->
        val name = prefix + wrapName(predicate.field.name)
        val value = wrapValue(value = predicate.value, dataType = predicate.field.dataType)
        when (predicate.operator) {
          Operator.Equals -> if (predicate.value.value == null) {
            "($name = $value OR $name IS NULL)"
          } else {
            "$name = $value"
          }
          Operator.GreaterThan -> if (predicate.value.value == null) {
            null
          } else {
            "($name > $value OR $name IS NULL)"
          }
          Operator.LessThan -> if (predicate.value.value == null) {
            null
          } else {
            "($name < $value OR $name IS NULL)"
          }
          Operator.GreaterThanOrEqualTo -> if (predicate.value.value == null) {
            null
          } else {
            "($name >= $value OR $name IS NULL)"
          }
          Operator.LessThanOrEqualTo -> if (predicate.value.value == null) {
            "$name IS NULL"
          } else {
            "($name <= $value OR $name IS NULL)"
          }
        }
      }.joinToString(" AND ")
      if (andClause == "") {
        null
      } else {
        andClause
      }
    }.joinToString(" OR ")
    return if (orClause == "") {
      null
    } else {
      orClause
    }
  }

  override fun wrapName(name: String) = "\"${name.lowercase()}\""

  override fun wrapValue(value: Value<*>, dataType: DataType<*>): String =
    when (value) {
      is Value.bool -> wrapBoolValue(value.value)
      is Value.decimal -> {
        if (dataType is DataType.decimal) {
          wrapDecimalValue(value.value, precision = dataType.precision, scale = dataType.scale)
        } else {
          throw KDAError.ValueDataTypeMismatch(value = value, dataType = dataType)
        }
      }
      is Value.float -> if (dataType is DataType.float) {
        wrapFloatValue(value.value, maxDigits = dataType.maxDigits)
      } else {
        throw KDAError.ValueDataTypeMismatch(value = value, dataType = dataType)
      }
      is Value.int -> wrapIntValue(value.value)
      is Value.date -> wrapLocalDateValue(value.value)
      is Value.datetime -> wrapLocalDateTimeValue(value.value)
      is Value.text -> if (dataType is DataType.text) {
        wrapStringValue(value.value, maxLength = dataType.maxLength)
      } else {
        throw KDAError.ValueDataTypeMismatch(value = value, dataType = dataType)
      }
      is Value.nullableBool -> wrapBoolValue(value.value)
      is Value.nullableDecimal -> if (dataType is DataType.nullableDecimal) {
        wrapDecimalValue(value.value, precision = dataType.precision, scale = dataType.scale)
      } else {
        throw KDAError.ValueDataTypeMismatch(value = value, dataType = dataType)
      }
      is Value.nullableFloat -> if (dataType is DataType.nullableFloat) {
        wrapFloatValue(value.value, maxDigits = dataType.maxDigits)
      } else {
        throw KDAError.ValueDataTypeMismatch(value = value, dataType = dataType)
      }
      is Value.nullableInt -> wrapIntValue(value.value)
      is Value.nullableDate -> wrapLocalDateValue(value.value)
      is Value.nullableDatetime -> wrapLocalDateTimeValue(value.value)
      is Value.nullableText -> if (dataType is DataType.nullableText) {
        wrapStringValue(value = value.value, maxLength = dataType.maxLength)
      } else {
        throw KDAError.ValueDataTypeMismatch(value = value, dataType = dataType)
      }
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
        DataType.bool -> "BOOL NOT NULL"
        is DataType.decimal -> "DECIMAL(${field.dataType.precision}, ${field.dataType.scale}) NOT NULL"
        is DataType.float -> "FLOAT NOT NULL"
        is DataType.int -> "INT NOT NULL"
        DataType.localDateTime -> "TIMESTAMP NOT NULL"
        DataType.localDate -> "DATE NOT NULL"
        is DataType.text -> "TEXT NULL"
        DataType.nullableBool -> "BOOL NULL"
        is DataType.nullableDecimal ->
          "DECIMAL(${field.dataType.precision}, ${field.dataType.scale}) NULL"
        is DataType.nullableFloat -> "FLOAT NULL"
        DataType.nullableLocalDateTime -> "TIMESTAMP NULL"
        DataType.nullableLocalDate -> "DATE NULL"
        is DataType.nullableInt -> "INT NULL"
        is DataType.nullableText -> "TEXT NULL"
      }
    return "$wrappedFieldName $dataType"
  }

  override fun maxValue(fieldName: String): String {
    val fldName = wrapName(fieldName)
    return "MAX($fldName) AS $fldName"
  }

  override fun valuesExpression(
    fields: Set<Field>,
    rows: Set<Row>,
    tableAlias: String?,
  ): String {
    val sortedFieldNames = fields.map { it.name }.sorted()
    val dataTypes = fields.associate { it.name to it.dataType }
    return rows.joinToString(", ") { row ->
      rowValuesExpression(
        sortedFieldNames = sortedFieldNames,
        row = row,
        tableAlias = tableAlias,
        dataTypes = dataTypes,
      )
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

  override fun valuesCTE(cteName: String, fields: Set<Field>, rows: Set<Row>): String {
    val fieldNames = fields.map { it.name }.toSet()
    val fieldsCSV = fieldNameCSV(fieldNames = fieldNames, tableAlias = null)
    val valuesExpr = valuesExpression(
      fields = fields, rows = rows, tableAlias = null
    )
    return "$cteName ($fieldsCSV) AS (VALUES $valuesExpr)"
  }

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

  private fun rowValuesExpression(
    sortedFieldNames: List<String>,
    row: Row,
    tableAlias: String? = null,
    dataTypes: Map<String, DataType<*>>,
  ): String {
    val valueCSV = sortedFieldNames.joinToString(", ") { fldName ->
      if (tableAlias == null) {
        val dataType = dataTypes[fldName]
        if (dataType == null) {
          throw KDAError.FieldNotFound(fieldName = fldName, availableFieldNames = dataTypes.keys)
        } else {
          wrapValue(value = row.value(fldName), dataType = dataType)
        }
      } else {
        val dataType = dataTypes[fldName]
        if (dataType == null) {
          throw KDAError.FieldNotFound(fieldName = fldName, availableFieldNames = dataTypes.keys)
        } else {
          "$tableAlias.${wrapValue(value = row.value(fldName), dataType = dataType)}"
        }
      }
    }
    return "($valueCSV)"
  }
}
