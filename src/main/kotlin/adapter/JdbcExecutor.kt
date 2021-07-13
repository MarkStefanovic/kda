package adapter

import domain.*
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.LocalDateTime

class JdbcExecutor(private val con: Connection) : SQLExecutor {
    override fun execute(sql: String) {
        with(con) { createStatement().use { stmt -> stmt.execute(sql) } }
    }

    override fun fetchNullableBool(sql: String): Boolean? = fetchScalar(sql) { v -> v as Boolean? }

    override fun fetchBool(sql: String): Boolean {
        val value = fetchNullableBool(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDate(sql: String): LocalDate? = fetchScalar(sql) { v -> (v as Date?)?.toLocalDate() }

    override fun fetchDate(sql: String): LocalDate {
        val value = fetchNullableDate(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDateTime(sql: String): LocalDateTime? =
        fetchScalar(sql) { v -> (v as Timestamp?)?.toLocalDateTime() }

    override fun fetchDateTime(sql: String): LocalDateTime {
        val value = fetchNullableDateTime(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDecimal(sql: String): BigDecimal? = fetchScalar(sql) { v -> v as BigDecimal? }

    override fun fetchDecimal(sql: String): BigDecimal {
        val value = fetchNullableDecimal(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableFloat(sql: String): Float? = fetchScalar(sql) { v -> (v as Double?)?.toFloat() }

    override fun fetchFloat(sql: String): Float {
        val value = fetchNullableFloat(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableInt(sql: String): Int? = fetchScalar(sql) { v -> if (v is Long) v.toInt() else v as Int? }

    override fun fetchInt(sql: String): Int {
        val value = fetchNullableInt(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableString(sql: String): String? = fetchScalar(sql) { v -> v as String? }

    override fun fetchString(sql: String): String {
        val value = fetchNullableString(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchRow(sql: String, fields: Set<Field>): Row = con.createStatement().use { stmt ->
        stmt.executeQuery(sql).use { rs ->
            rs.next()
            Row(rs.toMap(fields = fields))
        }
    }

    override fun fetchRows(sql: String, fields: Set<Field>): List<Row> = con.createStatement().use { stmt ->
        val rows: MutableList<Row> = mutableListOf()
        stmt.executeQuery(sql).use { rs ->
            while (rs.next()) {
                rows.add(Row(rs.toMap(fields = fields)))
            }
        }
        return rows
    }

    private inline fun <reified T : Any> fetchScalar(sql: String, cast: (v: Any?) -> T?) =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0) throw NoRowsReturned(sql = sql) else throw e
                }

                try {
                    when (value) {
                        null -> null
                        is T -> value
                        else -> cast(value)
                    }
                } catch (e: ClassCastException) {
                    throw ValueError(value = value, expectedType = T::class.simpleName ?: "Unknown")
                }
            }
        }
}

private fun ResultSet.toMap(fields: Set<Field>): Map<String, Value<*>> = fields.associate { fld ->
    fld.name to when (fld.dataType) {
        BoolType                  -> BoolValue(value = getBoolean(fld.name))
        NullableBoolType          -> NullableBoolValue(value = getObject(fld.name) as Boolean?)
        is DecimalType            -> DecimalValue(value = getBigDecimal(fld.name),
                                                  precision = fld.dataType.precision,
                                                  scale = fld.dataType.scale)
        is NullableDecimalType    -> NullableDecimalValue(value = getObject(fld.name) as BigDecimal?,
                                                          precision = fld.dataType.precision,
                                                          scale = fld.dataType.scale)
        is FloatType              -> FloatValue(value = getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
        is NullableFloatType      -> FloatValue(value = getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
        is IntType                -> IntValue(getInt(fld.name))
        is NullableIntType        -> NullableIntValue(getObject(fld.name) as Int?)
        LocalDateTimeType         -> LocalDateTimeValue(value = getTimestamp(fld.name).toLocalDateTime())
        NullableLocalDateTimeType -> NullableLocalDateTimeValue(value = getTimestamp(fld.name)?.toLocalDateTime())
        LocalDateType             -> LocalDateValue(value = getDate(fld.name).toLocalDate())
        NullableLocalDateType     -> NullableLocalDateValue(value = getDate(fld.name)?.toLocalDate())
        is StringType             -> StringValue(value = getString(fld.name), maxLength = fld.dataType.maxLength)
        is NullableStringType     -> StringValue(value = getString(fld.name), maxLength = fld.dataType.maxLength)
    }
}

//private fun rsToRow(fields: Set<Field>, rs: ResultSet): Row {
//    val valueMap: MutableMap<String, Value<*>> = mutableMapOf()
//    for (fld in fields) {
//        valueMap[fld.name] =
//            when (fld.dataType) {
//                BoolType -> BoolValue(value = rs.getBoolean(fld.name))
//                NullableBoolType -> NullableBoolValue(value = rs.getObject(fld.name) as Boolean?)
//                is DecimalType ->
//                    DecimalValue(
//                        value = rs.getBigDecimal(fld.name),
//                        precision = fld.dataType.precision,
//                        scale = fld.dataType.scale
//                    )
//                is NullableDecimalType ->
//                    NullableDecimalValue(
//                        value = rs.getObject(fld.name) as BigDecimal?,
//                        precision = fld.dataType.precision,
//                        scale = fld.dataType.scale
//                    )
//                is FloatType ->
//                    FloatValue(value = rs.getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
//                is NullableFloatType ->
//                    FloatValue(value = rs.getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
//                is IntType -> IntValue(rs.getInt(fld.name))
//                is NullableIntType -> NullableIntValue(rs.getObject(fld.name) as Int?)
//                LocalDateTimeType ->
//                    LocalDateTimeValue(value = rs.getTimestamp(fld.name).toLocalDateTime())
//                NullableLocalDateTimeType ->
//                    NullableLocalDateTimeValue(value = rs.getTimestamp(fld.name)?.toLocalDateTime())
//                LocalDateType -> LocalDateValue(value = rs.getDate(fld.name).toLocalDate())
//                NullableLocalDateType ->
//                    NullableLocalDateValue(value = rs.getDate(fld.name)?.toLocalDate())
//                is StringType ->
//                    StringValue(value = rs.getString(fld.name), maxLength = fld.dataType.maxLength)
//                is NullableStringType ->
//                    StringValue(value = rs.getString(fld.name), maxLength = fld.dataType.maxLength)
//            }
//    }
//    return Row(valueMap)
//}
