package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

sealed class DataType<T : Any?> {
  abstract val nullable: Boolean

  abstract val zeroValue: T

  abstract val name: DataTypeName

  abstract fun wrapValue(value: Any?): Value<T>

  object bool : DataType<Boolean>() {
    override val nullable: Boolean = false

    override val zeroValue: Boolean = false

    override val name = DataTypeName.Bool

    override fun wrapValue(value: Any?) = Value.bool(value as Boolean)
  }

  data class int(val autoincrement: Boolean) : DataType<Int>() {
    override val nullable: Boolean = false

    override val zeroValue: Int = 0

    override val name = DataTypeName.Int

    override fun wrapValue(value: Any?) = Value.int(value as Int)
  }

  object localDate : DataType<LocalDate>() {
    override val nullable: Boolean = false

    override val zeroValue: LocalDate = LocalDate.MIN

    override val name = DataTypeName.Date

    override fun wrapValue(value: Any?) = Value.date(value as LocalDate)
  }

  object localDateTime : DataType<LocalDateTime>() {
    override val nullable: Boolean = false

    override val zeroValue: LocalDateTime = LocalDateTime.MIN

    override val name = DataTypeName.DateTime

    override fun wrapValue(value: Any?) = Value.datetime(value as LocalDateTime)
  }

  data class float(val maxDigits: Int) : DataType<Float>() {
    override val nullable: Boolean = false

    override val zeroValue: Float = Float.MIN_VALUE

    override val name = DataTypeName.Float

    override fun wrapValue(value: Any?) = Value.float(value as Float)
  }

  data class decimal(val precision: Int, val scale: Int) : DataType<BigDecimal>() {
    override val nullable: Boolean = false

    override val zeroValue: BigDecimal = BigDecimal.ZERO

    override val name = DataTypeName.Decimal

    override fun wrapValue(value: Any?) = Value.decimal(value as BigDecimal)
  }

  data class text(val maxLength: Int?) : DataType<String>() {
    override val nullable: Boolean = false

    override val zeroValue: String = ""

    override val name = DataTypeName.Text

    override fun wrapValue(value: Any?) = Value.text(value as String)
  }

  object nullableBool : DataType<Boolean?>() {
    override val nullable: Boolean = true

    override val zeroValue: Boolean? = null

    override val name = DataTypeName.NullableBool

    override fun wrapValue(value: Any?) = Value.nullableBool(value as Boolean?)
  }

  data class nullableInt(val autoincrement: Boolean) : DataType<Int?>() {
    override val nullable: Boolean = true

    override val zeroValue: Int? = null

    override val name = DataTypeName.NullableInt

    override fun wrapValue(value: Any?) = Value.nullableInt(value as Int?)
  }

  object nullableLocalDate : DataType<LocalDate?>() {
    override val nullable: Boolean = true

    override val zeroValue: LocalDate? = LocalDate.MIN

    override val name = DataTypeName.NullableDate

    override fun wrapValue(value: Any?) = Value.nullableDate(value as LocalDate?)
  }

  object nullableLocalDateTime : DataType<LocalDateTime?>() {
    override val nullable: Boolean = true

    override val zeroValue: LocalDateTime? = null

    override val name = DataTypeName.NullableDateTime

    override fun wrapValue(value: Any?) = Value.nullableDatetime(value as LocalDateTime?)
  }

  data class nullableFloat(val maxDigits: Int) : DataType<Float?>() {
    override val nullable: Boolean = true

    override val zeroValue: Float? = null

    override val name = DataTypeName.NullableFloat

    override fun wrapValue(value: Any?) = Value.nullableFloat(value as Float?)
  }

  data class nullableDecimal(val precision: Int, val scale: Int) : DataType<BigDecimal?>() {
    override val nullable: Boolean = true

    override val zeroValue: BigDecimal? = null

    override val name = DataTypeName.NullableDecimal

    override fun wrapValue(value: Any?) = Value.nullableDecimal(value as BigDecimal?)
  }

  data class nullableText(val maxLength: Int?) : DataType<String?>() {
    override val nullable: Boolean = true

    override val zeroValue: String? = null

    override val name = DataTypeName.NullableText

    override fun wrapValue(value: Any?) = Value.nullableText(value as String?)
  }
}
