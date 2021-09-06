package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

sealed class DataType<out T : Any?> {
  abstract val nullable: Boolean

  abstract val zeroValue: T

  abstract val name: DataTypeName
}

object BoolType : DataType<Boolean>() {
  override val nullable: Boolean = false

  override val zeroValue: Boolean = false

  override val name = DataTypeName.Bool
}

data class IntType(val autoincrement: Boolean) : DataType<Int>() {
  override val nullable: Boolean = false

  override val zeroValue: Int = 0

  override val name = DataTypeName.Int
}

object LocalDateType : DataType<LocalDate>() {
  override val nullable: Boolean = false

  override val zeroValue: LocalDate = LocalDate.MIN

  override val name = DataTypeName.Date
}

object LocalDateTimeType : DataType<LocalDateTime>() {
  override val nullable: Boolean = false

  override val zeroValue: LocalDateTime = LocalDateTime.MIN

  override val name = DataTypeName.DateTime
}

data class FloatType(val maxDigits: Int) : DataType<Float>() {
  override val nullable: Boolean = false

  override val zeroValue: Float = Float.MIN_VALUE

  override val name = DataTypeName.Float
}

data class DecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal>() {
  override val nullable: Boolean = false

  override val zeroValue: BigDecimal = BigDecimal.ZERO

  override val name = DataTypeName.Decimal
}

data class StringType(val maxLength: Int?) : DataType<String>() {
  override val nullable: Boolean = false

  override val zeroValue: String = ""

  override val name = DataTypeName.Text
}

object NullableBoolType : DataType<Boolean?>() {
  override val nullable: Boolean = true

  override val zeroValue: Boolean? = null

  override val name = DataTypeName.NullableBool
}

data class NullableIntType(val autoincrement: Boolean) : DataType<Int?>() {
  override val nullable: Boolean = true

  override val zeroValue: Int? = null

  override val name = DataTypeName.NullableInt
}

object NullableLocalDateType : DataType<LocalDate?>() {
  override val nullable: Boolean = true

  override val zeroValue: LocalDate? = LocalDate.MIN

  override val name = DataTypeName.NullableDate
}

object NullableLocalDateTimeType : DataType<LocalDateTime?>() {
  override val nullable: Boolean = true

  override val zeroValue: LocalDateTime? = null

  override val name = DataTypeName.NullableDateTime
}

data class NullableFloatType(val maxDigits: Int) : DataType<Float?>() {
  override val nullable: Boolean = true

  override val zeroValue: Float? = null

  override val name = DataTypeName.NullableFloat
}

data class NullableDecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal?>() {
  override val nullable: Boolean = true

  override val zeroValue: BigDecimal? = null

  override val name = DataTypeName.NullableDecimal
}

data class NullableStringType(val maxLength: Int?) : DataType<String?>() {
  override val nullable: Boolean = true

  override val zeroValue: String? = null

  override val name = DataTypeName.NullableText
}
