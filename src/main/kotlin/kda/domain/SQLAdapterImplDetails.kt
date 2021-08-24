package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

interface SQLAdapterImplDetails {
    fun fieldDef(field: Field): String

    fun wrapName(name: String): String

    fun wrapValue(value: Value<*>): String

    fun wrapBoolValue(value: Boolean?): String

    fun wrapDecimalValue(value: BigDecimal?, scale: Int): String

    fun wrapFloatValue(value: Float?, maxDigits: Int): String

    fun wrapIntValue(value: Int?): String

    fun wrapLocalDateValue(value: LocalDate?): String

    fun wrapLocalDateTimeValue(value: LocalDateTime?): String

    fun wrapStringValue(value: String?, maxLength: Int?): String

    fun valuesExpression(fieldNames: List<String>, rows: Set<Row>): String
}