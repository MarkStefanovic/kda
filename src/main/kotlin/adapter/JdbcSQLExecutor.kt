package adapter

import domain.*
import java.sql.Connection
import java.time.LocalDate
import java.time.LocalDateTime

class JdbcSQLExecutor(private val con: Connection) : SQLExecutor {
    override fun execute(sql: String) {
        with (con) {
            createStatement().use { stmt ->
                stmt.execute(sql)
                commit()
            }
        }
    }

    override fun fetchNullableBool(sql: String): Boolean? {
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                return rs.getObject(0) as Boolean?
            }
        }
    }

    override fun fetchBool(sql: String): Boolean {
        val value = fetchNullableBool(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDate(sql: String): LocalDate? {
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                return rs.getDate(0)?.toLocalDate()
            }
        }
    }

    override fun fetchDate(sql: String): LocalDate {
        val value = fetchNullableDate(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableDateTime(sql: String): LocalDateTime? {
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                return rs.getTimestamp(0)?.toLocalDateTime()
            }
        }
    }

    override fun fetchDateTime(sql: String): LocalDateTime {
        val value = fetchNullableDateTime(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableFloat(sql: String): Float? {
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                return rs.getObject(0) as Float?
            }
        }
    }

    override fun fetchFloat(sql: String): Float {
        val value = fetchNullableFloat(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableInt(sql: String): Int? {
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                return rs.getObject(0) as Int?
            }
        }
    }

    override fun fetchInt(sql: String): Int {
        val value = fetchNullableInt(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchNullableString(sql: String): String? {
        con.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                rs.next()
                return rs.getString(0)
            }
        }
    }

    override fun fetchString(sql: String): String {
        val value = fetchNullableString(sql)
        require(value != null) { "The following query returned a null: $sql" }
        return value
    }

    override fun fetchRow(table: Table, sql: String): Row {
        TODO("Not yet implemented")
    }

    override fun fetchRows(table: Table, sql: String): Rows {
        val rs = con.createStatement().executeQuery(sql)
        val rows: MutableList<Row> = mutableListOf()
        while (rs.next()) {
            val valueMap: MutableMap<String, Value<*>> = mutableMapOf()
            for (fld in table.fields) {
                valueMap[fld.name] = when (fld.dataType) {
                    BoolType, NullableBoolType -> BoolValue(rs.getBoolean(fld.name))
                    is DecimalType, is NullableDecimalType -> DecimalValue(rs.getBigDecimal(fld.name))
                    FloatType, NullableFloatType -> FloatValue(rs.getFloat(fld.name))
                    LocalDateTimeType, NullableLocalDateTimeType -> LocalDateTimeValue(
                        rs.getTimestamp(fld.name).toLocalDateTime()
                    )
                    LocalDateType, NullableLocalDateType -> LocalDateValue(rs.getDate(fld.name).toLocalDate())
                    is StringType, is NullableStringType -> StringValue(rs.getString(fld.name))
                }
            }
            val row = Row(valueMap)
            rows.add(row)
        }
        return Rows(rows)
    }
}