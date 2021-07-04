package domain

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

    fun fetchNullableFloat(sql: String): Float?
    fun fetchFloat(sql: String): Float

    fun fetchNullableInt(sql: String): Int?
    fun fetchInt(sql: String): Int

    fun fetchNullableString(sql: String): String?
    fun fetchString(sql: String): String

    fun fetchRow(table: Table, sql: String): Row

    fun fetchRows(table: Table, sql: String): Rows
}