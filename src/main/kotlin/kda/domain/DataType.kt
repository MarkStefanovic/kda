@file:Suppress("ClassName")

package kda.domain

import java.math.BigDecimal
import java.sql.JDBCType
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime

sealed class DataType<out T : Any?>(
  open val description: String,
  open val jdbcType: JDBCType,
  open val nullable: Boolean,
  open val name: String,
) {

  object bool : DataType<Boolean>(description = "bool", jdbcType = JDBCType.BOOLEAN, nullable = false, name = "bool")
  object nullableBool : DataType<Boolean?>(description = "nullableBool", jdbcType = JDBCType.BOOLEAN, nullable = true, name = "nullableBool")

  object bigInt : DataType<Long>(description = "bigInt", jdbcType = JDBCType.BIGINT, nullable = false, name = "bigInt")
  object nullableBigInt : DataType<Long?>(description = "nullableBigInt", jdbcType = JDBCType.BIGINT, nullable = true, name = "nullableBigInt")

  data class decimal(val precision: Int, val scale: Int) : DataType<BigDecimal>(description = "decimal [ precision: $precision, scale: $scale ]", jdbcType = JDBCType.DECIMAL, nullable = false, name = "decimal")
  data class nullableDecimal(val precision: Int, val scale: Int) : DataType<BigDecimal?>(description = "nullableDecimal [ precision: $precision, scale: $scale ]", jdbcType = JDBCType.DECIMAL, nullable = true, name = "nullableDecimal")

  object float : DataType<Float>(description = "float", jdbcType = JDBCType.FLOAT, nullable = false, name = "float")
  object nullableFloat : DataType<Float?>(description = "nullableFloat", jdbcType = JDBCType.FLOAT, nullable = true, name = "nullableFloat")

  object int : DataType<Int>(description = "int", jdbcType = JDBCType.INTEGER, nullable = false, name = "int")
  object nullableInt : DataType<Int?>(description = "nullableInt", jdbcType = JDBCType.INTEGER, nullable = true, name = "nullableInt")

  object localDate : DataType<LocalDate>(description = "localDate", jdbcType = JDBCType.DATE, nullable = false, name = "localDate")
  object nullableLocalDate : DataType<LocalDate?>(description = "nullableLocalDate", jdbcType = JDBCType.DATE, nullable = true, name = "nullableLocalDate")

  data class text(val maxLength: Int? = null) : DataType<String>(description = "text [ maxLength: $maxLength ]", jdbcType = JDBCType.VARCHAR, nullable = false, name = "text")
  data class nullableText(val maxLength: Int? = null) : DataType<String?>(description = "nullableText [ maxLength: $maxLength ]", jdbcType = JDBCType.VARCHAR, nullable = true, name = "nullableText")

  data class timestamp(val precision: Int) : DataType<LocalDateTime>(description = "timestamp [ precision: $precision ]", jdbcType = JDBCType.TIMESTAMP, nullable = false, name = "timestamp")
  data class nullableTimestamp(val precision: Int) : DataType<LocalDateTime?>(description = "nullableTimestamp [ precision: $precision ]", jdbcType = JDBCType.TIMESTAMP, nullable = true, name = "nullableTimestamp")

  data class timestampUTC(val precision: Int) : DataType<OffsetDateTime>(description = "timestampUTC [ precision: $precision ]", jdbcType = JDBCType.TIMESTAMP_WITH_TIMEZONE, nullable = false, name = "timestampUTC")
  data class nullableTimestampUTC(val precision: Int) : DataType<OffsetDateTime?>(description = "nullableTimestampUTC [ precision: $precision ]", jdbcType = JDBCType.TIMESTAMP_WITH_TIMEZONE, nullable = true, name = "nullableTimestampUTC")

  override fun toString(): String = description
}
