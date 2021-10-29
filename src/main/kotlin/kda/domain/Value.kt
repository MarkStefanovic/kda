package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

sealed interface Value<out V : Any?> {
  val value: V

  @JvmInline value class bool(override val value: Boolean) : Value<Boolean>

  @JvmInline value class decimal(override val value: BigDecimal) : Value<BigDecimal>

  @JvmInline value class float(override val value: Float) : Value<Float>

  @JvmInline value class int(override val value: Int) : Value<Int>

  @JvmInline value class date(override val value: LocalDate) : Value<LocalDate>

  @JvmInline value class datetime(override val value: LocalDateTime) : Value<LocalDateTime>

  @JvmInline value class text(override val value: String) : Value<String>

  @JvmInline value class nullableBool(override val value: Boolean?) : Value<Boolean?>

  @JvmInline value class nullableDecimal(override val value: BigDecimal?) : Value<BigDecimal?>

  @JvmInline value class nullableFloat(override val value: Float?) : Value<Float?>

  @JvmInline value class nullableInt(override val value: Int?) : Value<Int?>

  @JvmInline value class nullableText(override val value: String?) : Value<String?>

  @JvmInline value class nullableDate(override val value: LocalDate?) : Value<LocalDate?>

  @JvmInline value class nullableDatetime(override val value: LocalDateTime?) : Value<LocalDateTime?>
}
