package adapter

import domain.*
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.reflect.typeOf

class JdbcExecutor(private val con: Connection) : SQLExecutor {
    override fun execute(sql: String) {
        with(con) {
            createStatement().use { stmt ->
                stmt.execute(sql)
            }
        }
    }

    override fun fetchNullableBool(sql: String): Boolean? =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()

                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0 )
                        throw NoRowsReturned(sql)
                    else
                        throw e
                }

                try {
                    value as Boolean?
                } catch(e: ClassCastException) {
                    throw NotABool(value)
                }
            }
        }

    override fun fetchBool(sql: String): Boolean {
        val value = fetchNullableBool(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDate(sql: String): LocalDate? =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()

                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0 )
                        throw NoRowsReturned(sql)
                    else
                        throw e
                }

                try {
                    (value as Date?)?.toLocalDate()
                } catch(e: ClassCastException) {
                    throw NotADate(value)
                }
            }
        }

    override fun fetchDate(sql: String): LocalDate {
        val value = fetchNullableDate(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDateTime(sql: String): LocalDateTime? =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()

                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0 )
                        throw NoRowsReturned(sql)
                    else
                        throw e
                }
                try {
                    (value as Timestamp?)?.toLocalDateTime()
                } catch(e: ClassCastException) {
                    throw NotATimestamp(value)
                }
            }
        }

    override fun fetchDateTime(sql: String): LocalDateTime {
        val value = fetchNullableDateTime(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDecimal(sql: String): BigDecimal? =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()

                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0 )
                        throw NoRowsReturned(sql)
                    else
                        throw e
                }

                try {
                    value as BigDecimal?
                } catch(e: ClassCastException) {
                    throw NotADecimal(value)
                }
            }
        }

    override fun fetchDecimal(sql: String): BigDecimal {
        val value = fetchNullableDecimal(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableFloat(sql: String): Float? =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()

                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0 )
                        throw NoRowsReturned(sql)
                    else
                        throw e
                }

                try {
                    (value as Double?)?.toFloat()
                } catch(e: ClassCastException) {
                    throw NotAFloat(value)
                }
            }
        }

    override fun fetchFloat(sql: String): Float {
        val value = fetchNullableFloat(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableInt(sql: String): Int? =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()

                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0 )
                        throw NoRowsReturned(sql)
                    else
                        throw e
                }

                try {
                    if (value is Long)
                        value.toInt()
                    else
                        value as Int?
                } catch(e: ClassCastException) {
                    throw NotAnInt(value)
                }
            }
        }

    override fun fetchInt(sql: String): Int {
        val value = fetchNullableInt(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableString(sql: String): String? =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()

                val value = try {
                    rs.getObject(1)
                } catch (e: Exception) {
                    if (rs.metaData.columnCount == 0 )
                        throw NoRowsReturned(sql)
                    else
                        throw e
                }

                try {
                    value as String?
                } catch(e: ClassCastException) {
                    throw NotAString(value)
                }
            }
        }

    override fun fetchString(sql: String): String {
        val value = fetchNullableString(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchRow(sql: String, fields: Set<Field>): Row =
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                rsToRow(fields = fields, rs = rs)
            }
        }

    override fun fetchRows(sql: String, fields: Set<Field>): List<Row> =
        con.createStatement().use { stmt ->
            val rows: MutableList<Row> = mutableListOf()
            stmt.executeQuery(sql).use { rs ->
                while (rs.next()) {
                    val row = rsToRow(fields = fields, rs = rs)
                    rows.add(row)
                }
            }
            return rows
        }
}

private fun rsToRow(fields: Set<Field>, rs: ResultSet): Row {
    val valueMap: MutableMap<String, Value<*>> = mutableMapOf()
    for (fld in fields) {
        valueMap[fld.name] = when (fld.dataType) {
            BoolType -> BoolValue(value = rs.getBoolean(fld.name))
            NullableBoolType -> NullableBoolValue(value = rs.getObject(fld.name) as Boolean?)
            is DecimalType -> DecimalValue(
                value = rs.getBigDecimal(fld.name),
                precision = fld.dataType.precision,
                scale = fld.dataType.scale
            )
            is NullableDecimalType -> NullableDecimalValue(
                value = rs.getObject(fld.name) as BigDecimal?,
                precision = fld.dataType.precision,
                scale = fld.dataType.scale
            )
            is FloatType -> FloatValue(value = rs.getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
            is NullableFloatType -> FloatValue(value = rs.getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
            is IntType -> IntValue(rs.getInt(fld.name))
            is NullableIntType -> NullableIntValue(rs.getObject(fld.name) as Int?)
            LocalDateTimeType -> LocalDateTimeValue(
                value = rs.getTimestamp(fld.name).toLocalDateTime()
            )
            NullableLocalDateTimeType -> NullableLocalDateTimeValue(
                value = rs.getTimestamp(fld.name)?.toLocalDateTime()
            )
            LocalDateType -> LocalDateValue(value = rs.getDate(fld.name).toLocalDate())
            NullableLocalDateType -> NullableLocalDateValue(value = rs.getDate(fld.name)?.toLocalDate())
            is StringType -> StringValue(value = rs.getString(fld.name), maxLength = fld.dataType.maxLength)
            is NullableStringType -> StringValue(value = rs.getString(fld.name), maxLength = fld.dataType.maxLength)
        }
    }
    return Row(valueMap)
}