package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

interface SQLAdapterImplDetails {
  fun fieldDef(field: Field): String

  fun fieldNameCSV(fieldNames: Set<String>, tableAlias: String? = null): String

  fun fullTableName(schema: String?, table: String): String

  fun maxValue(fieldName: String): String

  fun joinFields(table: Table, leftTableAlias: String, rightTableAlias: String): String

  fun renderCriteria(criteria: Set<Criteria>): String

  fun setValues(table: Table, rightTableAlias: String): String

  fun wrapName(name: String): String

  fun wrapValue(value: Value<*>): String

  fun wrapBoolValue(value: Boolean?): String

  fun wrapDecimalValue(value: BigDecimal?, precision: Int, scale: Int): String

  fun wrapFloatValue(value: Float?, maxDigits: Int): String

  fun wrapIntValue(value: Int?): String

  fun wrapLocalDateValue(value: LocalDate?): String

  fun wrapLocalDateTimeValue(value: LocalDateTime?): String

  fun wrapStringValue(value: String?, maxLength: Int?): String

  fun valuesExpression(fieldNames: List<String>, rows: Set<Row>, tableAlias: String? = null): String
}
