package domain

import java.math.BigDecimal
import java.math.RoundingMode
import java.text.DecimalFormat


interface SQLAdapterImplDetails {
    fun fieldDef(field: Field): String

    fun wrapName(name: String): String

    fun wrapValue(value: Value<*>): String

    fun valuesExpression(rows: Rows): String
}

class StandardSQLAdapterImplDetails(private val keywords: Set<String>): SQLAdapterImplDetails {
    private val standardizedKeywords: Set<String> by lazy {
        keywords.map { kw -> kw.lowercase() }.toSet()
    }

    override fun wrapName(name: String): String {
        val n = name.lowercase()
        return if (n in standardizedKeywords)
            "\"$n\""
        else
            n
    }

    override fun wrapValue(value: Value<*>): String =
        when (value) {
            is BoolValue -> if (value.value) "TRUE" else "FALSE"
            is DecimalValue -> formatDecimal(value = value.value, scale = value.scale)
            is FloatValue -> "%.${value.maxDigits}f".format(value.value)
            is LocalDateTimeValue -> value.value.toString()
            is LocalDateValue -> value.value.toString()
            is StringValue -> {
                if (value.maxLength == null)
                    value.value
                else
                    value.value.substring(0, value.maxLength)
            }
            is NullableBoolValue ->
                when (value.value) {
                    null -> "NULL"
                    true -> "TRUE"
                    else -> "FALSE"
                }
            is NullableDecimalValue ->
                if (value.value == null)
                    "NULL"
                else
                    formatDecimal(value = value.value, scale = value.scale)
            is NullableFloatValue ->
                if (value.value == null)
                    "NULL"
                else
                    "%.${value.maxDigits}f".format(value.value)
            is NullableLocalDateTimeValue ->
                if (value.value == null)
                    "NULL"
                else
                    value.value.toString()
            is NullableLocalDateValue ->
                if (value.value == null)
                    "NULL"
                else
                    value.value.toString()
            is NullableStringValue ->
                when {
                    value.value == null -> "NULL"
                    value.maxLength == null -> value.value.toString()
                    else -> value.value.toString()
                }
        }

    override fun fieldDef(field: Field): String {
        val wrappedFieldName = wrapName(field.name)
        val dataType = when (field.dataType) {
            BoolType -> "BOOL NOT NULL"
            is DecimalType -> "DECIMAL(${field.dataType.precision}, ${field.dataType.scale}) NOT NULL"
            FloatType -> "FLOAT NOT NULL"
            LocalDateTimeType -> "TIMESTAMP NOT NULL"
            LocalDateType -> "DATE NOT NULL"
            is StringType -> "TEXT NULL"
            NullableBoolType -> "BOOL NULL"
            is NullableDecimalType -> "DECIMAL(${field.dataType.precision}, ${field.dataType.scale}) NULL"
            NullableFloatType -> "FLOAT NULL"
            NullableLocalDateTimeType -> "TIMESTAMP NULL"
            NullableLocalDateType -> "DATE NULL"
            is NullableStringType -> "TEXT NULL"
        }
        return "$wrappedFieldName $dataType"
    }

    override fun valuesExpression(rows: Rows): String {
        return rows.values.joinToString(", ") { row -> rowValuesExpression(row) }
    }

    private fun rowValuesExpression(row: Row): String {
        val valueCSV = row.values.joinToString(", ") { value -> wrapValue(value) }
        return "($valueCSV)"
    }
}


fun formatDecimal(value: BigDecimal, scale: Int): String =
    with (DecimalFormat("#.#")) {
        roundingMode = RoundingMode.CEILING
        maximumFractionDigits = scale
        format(value)
    }



