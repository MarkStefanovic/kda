package adapter.std

import domain.*
import java.math.BigDecimal
import java.math.RoundingMode
import java.text.DecimalFormat
import java.time.LocalDate
import java.time.LocalDateTime

class StdSQLAdapterImplDetails(private val keywords: Set<String>) : SQLAdapterImplDetails {
    private val standardizedKeywords: Set<String> by lazy {
        keywords.map { kw -> kw.lowercase() }.toSet()
    }

    override fun wrapName(name: String): String {
        val n = name.lowercase()
        return if (n in standardizedKeywords) "\"$n\"" else n
    }

    override fun wrapValue(value: Value<*>): String =
        when (value) {
            is BoolValue -> wrapBoolValue(value.value)
            is DecimalValue -> wrapDecimalValue(value.value, scale = value.scale)
            is FloatValue -> wrapFloatValue(value.value, maxDigits = value.maxDigits)
            is IntValue -> wrapIntValue(value.value)
            is LocalDateValue -> wrapLocalDateValue(value.value)
            is LocalDateTimeValue -> wrapLocalDateTimeValue(value.value)
            is StringValue -> wrapStringValue(value.value, maxLength = value.maxLength)
            is NullableBoolValue -> wrapBoolValue(value.value)
            is NullableDecimalValue -> wrapDecimalValue(value.value, scale = value.scale)
            is NullableFloatValue -> wrapFloatValue(value.value, maxDigits = value.maxDigits)
            is NullableIntValue -> wrapIntValue(value.value)
            is NullableLocalDateValue -> wrapLocalDateValue(value.value)
            is NullableLocalDateTimeValue -> wrapLocalDateTimeValue(value.value)
            is NullableStringValue ->
                wrapStringValue(value = value.value, maxLength = value.maxLength)
        }

    override fun wrapBoolValue(value: Boolean?): String =
        when {
            value == null -> "NULL"
            value -> "1"
            else -> "0"
        }

    override fun wrapDecimalValue(value: BigDecimal?, scale: Int): String =
        if (value == null) "NULL"
        else
            with(DecimalFormat("#.#")) {
                roundingMode = RoundingMode.CEILING
                maximumFractionDigits = scale
                format(value)
            }

    override fun wrapFloatValue(value: Float?, maxDigits: Int): String =
        if (value == null) "NULL" else "%.${maxDigits}f".format(value)

    override fun wrapIntValue(value: Int?): String = value?.toString() ?: "NULL"

    override fun wrapLocalDateValue(value: LocalDate?): String =
        if (value == null) "NULL" else "'$value'"

    override fun wrapLocalDateTimeValue(value: LocalDateTime?): String =
        if (value == null) "NULL" else "'$value'"

    override fun wrapStringValue(value: String?, maxLength: Int?): String =
        when {
            value == null -> "NULL"
            maxLength == null -> "'$value'"
            else -> "'${value.substring(0, kotlin.math.min(value.length, 40))}'"
        }

    override fun fieldDef(field: Field): String {
        val wrappedFieldName = wrapName(field.name)
        val dataType =
            when (field.dataType) {
                BoolType -> "BOOL NOT NULL"
                is DecimalType ->
                    "DECIMAL(${field.dataType.precision}, ${field.dataType.scale}) NOT NULL"
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

    override fun valuesExpression(fieldNames: List<String>, rows: IndexedRows): String {
        val sortedFieldNames = fieldNames.sorted()
        return rows.values.joinToString(", ") { row ->
            rowValuesExpression(sortedFieldNames = sortedFieldNames, row = row)
        }
    }

    private fun rowValuesExpression(sortedFieldNames: List<String>, row: Row): String {
        val valueCSV =
            sortedFieldNames.joinToString(", ") { fldName -> wrapValue(row.value(fldName)) }
        return "($valueCSV)"
    }
}
