package domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

sealed class DataType<out T : Any?> {
  abstract val nullable: Boolean

  abstract val zeroValue: T
}

object BoolType : DataType<Boolean>() {
  override val nullable: Boolean = false

  override val zeroValue: Boolean = false
}

data class IntType(val autoincrement: Boolean) : DataType<Int>() {
  override val nullable: Boolean = false

  override val zeroValue: Int = 0
}

object LocalDateType : DataType<LocalDate>() {
  override val nullable: Boolean = false

  override val zeroValue: LocalDate = LocalDate.MIN
}

object LocalDateTimeType : DataType<LocalDateTime>() {
  override val nullable: Boolean = false

  override val zeroValue: LocalDateTime = LocalDateTime.MIN
}

data class FloatType(val maxDigits: Int) : DataType<Float>() {
  override val nullable: Boolean = false

  override val zeroValue: Float = Float.MIN_VALUE
}

data class DecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal>() {
  override val nullable: Boolean = false

  override val zeroValue: BigDecimal = BigDecimal.ZERO
}

data class StringType(val maxLength: Int?) : DataType<String>() {
  override val nullable: Boolean = false

  override val zeroValue: String = ""
}

object NullableBoolType : DataType<Boolean?>() {
  override val nullable: Boolean = true

  override val zeroValue: Boolean? = null
}

data class NullableIntType(val autoincrement: Boolean) : DataType<Int?>() {
  override val nullable: Boolean = true

  override val zeroValue: Int? = null
}

object NullableLocalDateType : DataType<LocalDate?>() {
  override val nullable: Boolean = true

  override val zeroValue: LocalDate? = LocalDate.MIN
}

object NullableLocalDateTimeType : DataType<LocalDateTime?>() {
  override val nullable: Boolean = true

  override val zeroValue: LocalDateTime? = null
}

data class NullableFloatType(val maxDigits: Int) : DataType<Float?>() {
  override val nullable: Boolean = true

  override val zeroValue: Float? = null
}

data class NullableDecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal?>() {
  override val nullable: Boolean = true

  override val zeroValue: BigDecimal? = null
}

data class NullableStringType(val maxLength: Int?) : DataType<String?>() {
  override val nullable: Boolean = true

  override val zeroValue: String? = null
}
