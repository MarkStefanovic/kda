package domain

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

  fun fetchNullableDecimal(sql: String, precision: Int = 19, scale: Int = 2): BigDecimal?
  fun fetchDecimal(sql: String, precision: Int = 19, scale: Int = 2): BigDecimal

  fun fetchNullableFloat(sql: String, maxDigits: Int): Float?
  fun fetchFloat(sql: String, maxDigits: Int): Float

  fun fetchNullableInt(sql: String): Int?
  fun fetchInt(sql: String): Int

  fun fetchNullableString(sql: String, maxLength: Int?): String?
  fun fetchString(sql: String, maxLength: Int?): String

  fun fetchRow(sql: String, fields: Set<Field>): Row

  fun fetchRows(sql: String, fields: Set<Field>): Set<Row>
}
