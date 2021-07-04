package domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

sealed class DataType<out T: Any?> {
    abstract val zeroValue: T
}

object BoolType: DataType<Boolean>() {
    override val zeroValue: Boolean = false
}

object LocalDateType: DataType<LocalDate>() {
    override val zeroValue: LocalDate = LocalDate.MIN
}

object LocalDateTimeType: DataType<LocalDateTime>() {
    override val zeroValue: LocalDateTime = LocalDateTime.MIN
}

object FloatType: DataType<Float>() {
    override val zeroValue: Float = Float.MIN_VALUE
}

data class DecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal>() {
    override val zeroValue: BigDecimal = BigDecimal.ZERO
}

data class StringType(val maxLength: Int?): DataType<String>() {
    override val zeroValue: String = ""
}

object NullableBoolType: DataType<Boolean?>() {
    override val zeroValue: Boolean? = null
}

object NullableLocalDateType: DataType<LocalDate?>() {
    override val zeroValue: LocalDate? = LocalDate.MIN
}

object NullableLocalDateTimeType: DataType<LocalDateTime?>() {
    override val zeroValue: LocalDateTime? = null
}

object NullableFloatType: DataType<Float?>() {
    override val zeroValue: Float? = null
}

data class NullableDecimalType(val precision: Int, val scale: Int) : DataType<BigDecimal?>() {
    override val zeroValue: BigDecimal? = null
}

data class NullableStringType(val maxLength: Int?): DataType<String?>() {
    override val zeroValue: String? = null
}