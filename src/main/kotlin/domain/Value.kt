package domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

sealed interface Value<V: Any?> {
    val value: V
}

@JvmInline
value class NullableBoolValue(override val value: Boolean?): Value<Boolean?>

@JvmInline
value class BoolValue(override val value: Boolean): Value<Boolean>

data class NullableDecimalValue(override val value: BigDecimal?, val precision: Int, val scale: Int): Value<BigDecimal?>

data class DecimalValue(override val value: BigDecimal, val precision: Int, val scale: Int): Value<BigDecimal>

data class NullableFloatValue(override val value: Float?, val maxDigits: Int): Value<Float?>

data class FloatValue(override val value: Float, val maxDigits: Int): Value<Float>

@JvmInline
value class NullableLocalDateValue(override val value: LocalDate?): Value<LocalDate?>

@JvmInline
value class LocalDateValue(override val value: LocalDate): Value<LocalDate>

@JvmInline
value class NullableLocalDateTimeValue(override val value: LocalDateTime?): Value<LocalDateTime?>

@JvmInline
value class LocalDateTimeValue(override val value: LocalDateTime): Value<LocalDateTime>

data class NullableStringValue(override val value: String?, val maxLength: Int?): Value<String?>

data class StringValue(override val value: String, val maxLength: Int?): Value<String>

