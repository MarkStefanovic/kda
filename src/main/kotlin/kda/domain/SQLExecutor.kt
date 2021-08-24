package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

interface SQLExecutor {
    fun execute(sql: String)

    fun fetchNullableBool(sql: String): Boolean?
    fun fetchBool(sql: String): Boolean

    fun fetchNullableDate(sql: String): LocalDate?
    fun fetchDate(sql: String): LocalDate

    fun fetchNullableDateTime(sql: String): LocalDateTime?
    fun fetchDateTime(sql: String): LocalDateTime

    fun fetchNullableDecimal(sql: String): BigDecimal?
    fun fetchDecimal(sql: String): BigDecimal

    fun fetchNullableFloat(sql: String): Float?
    fun fetchFloat(sql: String): Float

    fun fetchNullableInt(sql: String): Int?
    fun fetchInt(sql: String): Int

    fun fetchNullableString(sql: String): String?
    fun fetchString(sql: String): String

    fun fetchRow(sql: String, fields: Set<Field>): Row

    fun fetchRows(sql: String, fields: Set<Field>): Set<Row>
}
