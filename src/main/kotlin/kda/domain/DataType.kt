package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

sealed class DataType<T : Any?> {
  abstract val nullable: Boolean

  abstract val zeroValue: T

  abstract val name: DataTypeName

  abstract fun wrapValue(value: Any?): Value<T>
}

object BoolType : DataType<Boolean>() {
  override val nullable: Boolean = false

  override val zeroValue: Boolean = false

  override val name = DataTypeName.Bool

  override fun wrapValue(value: Any?) = BoolValue(value as Boolean)
}

data class IntType(val autoincrement: Boolean) : DataType<Int>() {
  override val nullable: Boolean = false

  override val zeroValue: Int = 0

  override val name = DataTypeName.Int

  override fun wrapValue(value: Any?) =
    IntValue(value as Int)
}

object LocalDateType : DataType<LocalDate>() {
  override val nullable: Boolean = false

  override val zeroValue: LocalDate = LocalDate.MIN

  override val name = DataTypeName.Date

  override fun wrapValue(value: Any?) =
    LocalDateValue(value as LocalDate)
}

object LocalDateTimeType : DataType<LocalDateTime>() {
  override val nullable: Boolean = false

  override val zeroValue: LocalDateTime = LocalDateTime.MIN

  override val name = DataTypeName.DateTime

  override fun wrapValue(value: Any?) =
    LocalDateTimeValue(value as LocalDateTime)
}

data class FloatType(val maxDigits: Int) : DataType<Float>() {
  override val nullable: Boolean = false

  override val zeroValue: Float = Float.MIN_VALUE

  override val name = DataTypeName.Float

  override fun wrapValue(value: Any?) =
    FloatValue(value = value as Float, maxDigits = maxDigits)
}

data class DecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal>() {
  override val nullable: Boolean = false

  override val zeroValue: BigDecimal = BigDecimal.ZERO

  override val name = DataTypeName.Decimal

  override fun wrapValue(value: Any?) =
    DecimalValue(value = value as BigDecimal, precision = precision, scale = scale)
}

data class StringType(val maxLength: Int?) : DataType<String>() {
  override val nullable: Boolean = false

  override val zeroValue: String = ""

  override val name = DataTypeName.Text

  override fun wrapValue(value: Any?) =
    StringValue(value = value as String, maxLength = maxLength)
}

object NullableBoolType : DataType<Boolean?>() {
  override val nullable: Boolean = true

  override val zeroValue: Boolean? = null

  override val name = DataTypeName.NullableBool

  override fun wrapValue(value: Any?) =
    NullableBoolValue(value as Boolean?)
}

data class NullableIntType(val autoincrement: Boolean) : DataType<Int?>() {
  override val nullable: Boolean = true

  override val zeroValue: Int? = null

  override val name = DataTypeName.NullableInt

  override fun wrapValue(value: Any?) =
    NullableIntValue(value as Int?)
}

object NullableLocalDateType : DataType<LocalDate?>() {
  override val nullable: Boolean = true

  override val zeroValue: LocalDate? = LocalDate.MIN

  override val name = DataTypeName.NullableDate

  override fun wrapValue(value: Any?) =
    NullableLocalDateValue(value as LocalDate?)
}

object NullableLocalDateTimeType : DataType<LocalDateTime?>() {
  override val nullable: Boolean = true

  override val zeroValue: LocalDateTime? = null

  override val name = DataTypeName.NullableDateTime

  override fun wrapValue(value: Any?) =
    NullableLocalDateTimeValue(value as LocalDateTime?)
}

data class NullableFloatType(val maxDigits: Int) : DataType<Float?>() {
  override val nullable: Boolean = true

  override val zeroValue: Float? = null

  override val name = DataTypeName.NullableFloat

  override fun wrapValue(value: Any?) =
    NullableFloatValue(value = value as Float?, maxDigits = maxDigits)
}

data class NullableDecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal?>() {
  override val nullable: Boolean = true

  override val zeroValue: BigDecimal? = null

  override val name = DataTypeName.NullableDecimal

  override fun wrapValue(value: Any?) =
    NullableDecimalValue(value = value as BigDecimal?, precision = precision, scale = scale)
}

data class NullableStringType(val maxLength: Int?) : DataType<String?>() {
  override val nullable: Boolean = true

  override val zeroValue: String? = null

  override val name = DataTypeName.NullableText

  override fun wrapValue(value: Any?) =
    NullableStringValue(value = value as String?, maxLength = maxLength)
}
