package kda.domain

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime

@DslMarker annotation class CriteriaDSL

fun where(init: Or.Builder.() -> Unit): Set<Criteria> = Or.Builder().apply(init).build().criteria

fun and(init: And.Builder.() -> Unit): Set<Criteria> =
  setOf(Criteria(And.Builder().apply(init).build().predicates.toSet()))

data class Or(val criteria: Set<Criteria>) {
  @CriteriaDSL
  class Builder {
    private val andCriteria: MutableList<Criteria> = mutableListOf()

    fun build() = Or(andCriteria.toSet())

    fun and(criteria: And.Builder.() -> Unit) {
      andCriteria.add(Criteria(And.Builder().apply(criteria).build().predicates))
    }

    fun boolField(fieldName: String, init: BooleanFieldPredicate.Builder.() -> Unit): Criteria {
      val criteria =
        Criteria(BooleanFieldPredicate.Builder(fieldName).apply(init).build().predicates.toSet())
      andCriteria.add(criteria)
      return criteria
    }

    fun nullableBoolField(
      fieldName: String,
      init: NullableBooleanFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria =
        Criteria(
          NullableBooleanFieldPredicate.Builder(fieldName)
            .apply(init)
            .build()
            .predicates
            .toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun decimalField(
      fieldName: String,
      precision: Int = 19,
      scale: Int = 2,
      init: DecimalFieldPredicate.Builder.() -> Unit,
    ): Criteria {
      val criteria =
        Criteria(
          DecimalFieldPredicate.Builder(
            fieldName = fieldName,
            precision = precision,
            scale = scale,
          )
            .apply(init)
            .build()
            .predicates
            .toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun nullableDecimalField(
      fieldName: String,
      precision: Int = 19,
      scale: Int = 2,
      init: NullableDecimalFieldPredicate.Builder.() -> Unit,
    ): Criteria {
      val criteria =
        Criteria(
          NullableDecimalFieldPredicate.Builder(
            fieldName = fieldName,
            precision = precision,
            scale = scale,
          )
            .apply(init)
            .build()
            .predicates
            .toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun intField(fieldName: String, init: IntFieldPredicate.Builder.() -> Unit): Criteria {
      val criteria =
        Criteria(IntFieldPredicate.Builder(fieldName).apply(init).build().predicates.toSet())
      andCriteria.add(criteria)
      return criteria
    }

    fun nullableIntField(
      fieldName: String,
      init: NullableIntFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria =
        Criteria(
          NullableIntFieldPredicate.Builder(fieldName).apply(init).build().predicates.toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun floatField(
      fieldName: String,
      maxDigits: Int = 5,
      init: FloatFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria =
        Criteria(
          FloatFieldPredicate.Builder(fieldName = fieldName, maxDigits = maxDigits)
            .apply(init)
            .build()
            .predicates
            .toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun nullableFloatField(
      fieldName: String,
      maxDigits: Int = 5,
      init: NullableFloatFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria =
        Criteria(
          NullableFloatFieldPredicate.Builder(fieldName = fieldName, maxDigits = maxDigits)
            .apply(init)
            .build()
            .predicates
            .toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun dateField(fieldName: String, init: LocalDateFieldPredicate.Builder.() -> Unit): Criteria {
      val criteria =
        Criteria(
          LocalDateFieldPredicate.Builder(fieldName).apply(init).build().predicates.toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun nullableDateField(
      fieldName: String,
      init: NullableLocalDateTimeFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria =
        Criteria(
          NullableLocalDateTimeFieldPredicate.Builder(fieldName)
            .apply(init)
            .build()
            .predicates
            .toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun dateTimeField(
      fieldName: String,
      init: LocalDateFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria =
        Criteria(
          LocalDateFieldPredicate.Builder(fieldName).apply(init).build().predicates.toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun nullableDateTimeField(
      fieldName: String,
      init: NullableLocalDateTimeFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria =
        Criteria(
          NullableLocalDateTimeFieldPredicate.Builder(fieldName)
            .apply(init)
            .build()
            .predicates
            .toSet()
        )
      andCriteria.add(criteria)
      return criteria
    }

    fun textField(fieldName: String, init: TextFieldPredicate.Builder.() -> Unit): Criteria {
      val criteria = Criteria(TextFieldPredicate.Builder(fieldName).apply(init).build().predicates.toSet())
      andCriteria.add(criteria)
      return criteria
    }

    fun nullableTextField(
      fieldName: String,
      init: NullableTextFieldPredicate.Builder.() -> Unit
    ): Criteria {
      val criteria = Criteria(
        NullableTextFieldPredicate.Builder(fieldName)
          .apply(init)
          .build()
          .predicates
          .toSet()
      )
      andCriteria.add(criteria)
      return criteria
    }
  }
}

data class And(val predicates: Set<Predicate>) {
  @CriteriaDSL
  class Builder {
    private val orCriteria: MutableList<Predicate> = mutableListOf()

    fun build() = And(orCriteria.toSet())

    fun boolField(
      name: String,
      init: BooleanFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(BooleanFieldPredicate.Builder(name).apply(init).build().predicates)
    }

    fun nullableBoolField(
      name: String,
      init: NullableBooleanFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(NullableBooleanFieldPredicate.Builder(name).apply(init).build().predicates)
    }

    fun decimalField(
      name: String,
      precision: Int,
      scale: Int,
      init: DecimalFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(
        DecimalFieldPredicate.Builder(
          fieldName = name,
          precision = precision,
          scale = scale,
        )
          .apply(init)
          .build()
          .predicates
      )
    }

    fun nullableDecimalField(
      name: String,
      precision: Int,
      scale: Int,
      init: NullableDecimalFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(
        NullableDecimalFieldPredicate.Builder(
          fieldName = name,
          precision = precision,
          scale = scale,
        )
          .apply(init)
          .build()
          .predicates
      )
    }

    fun floatField(
      name: String,
      maxDigits: Int,
      init: FloatFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(
        FloatFieldPredicate.Builder(
          fieldName = name,
          maxDigits = maxDigits,
        )
          .apply(init)
          .build()
          .predicates
      )
    }

    fun nullableFloatField(
      name: String,
      maxDigits: Int,
      init: NullableFloatFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(
        NullableFloatFieldPredicate.Builder(
          fieldName = name,
          maxDigits = maxDigits,
        )
          .apply(init)
          .build()
          .predicates
      )
    }

    fun intField(
      name: String,
      init: IntFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(IntFieldPredicate.Builder(name).apply(init).build().predicates)
    }

    fun nullableIntField(
      name: String,
      init: NullableIntFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(NullableIntFieldPredicate.Builder(name).apply(init).build().predicates)
    }

    fun localDateField(
      name: String,
      init: LocalDateFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(LocalDateFieldPredicate.Builder(name).apply(init).build().predicates)
    }

    fun nullableLocalDateField(
      name: String,
      init: NullableLocalDateFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(
        NullableLocalDateFieldPredicate.Builder(name).apply(init).build().predicates
      )
    }

    fun localDateTimeField(
      name: String,
      init: LocalDateTimeFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(LocalDateTimeFieldPredicate.Builder(name).apply(init).build().predicates)
    }

    fun nullableLocalDateTimeField(
      name: String,
      init: NullableLocalDateTimeFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(
        NullableLocalDateTimeFieldPredicate.Builder(name).apply(init).build().predicates
      )
    }

    fun textField(
      name: String,
      init: TextFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(TextFieldPredicate.Builder(name).apply(init).build().predicates)
    }

    fun nullableTextField(
      name: String,
      init: NullableTextFieldPredicate.Builder.() -> Unit,
    ) {
      orCriteria.addAll(NullableTextFieldPredicate.Builder(name).apply(init).build().predicates)
    }
  }
}

data class BooleanFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = BooleanFieldPredicate(predicates)

    fun eq(value: Boolean) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = BoolType),
          value = Value.bool(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class NullableBooleanFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = NullableBooleanFieldPredicate(predicates)

    fun eq(value: Boolean?) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = NullableBoolType),
          value = Value.nullableBool(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class DecimalFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String, val precision: Int, val scale: Int) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = DecimalFieldPredicate(predicates)

    fun eq(value: BigDecimal) =
      predicates.add(
        Predicate(
          field =
          Field(
            name = fieldName,
            dataType = DecimalType(precision = precision, scale = scale)
          ),
          value = Value.decimal(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class NullableDecimalFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String, val precision: Int, val scale: Int) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = NullableDecimalFieldPredicate(predicates)

    fun eq(value: BigDecimal?) =
      predicates.add(
        Predicate(
          field =
          Field(
            name = fieldName,
            dataType = NullableDecimalType(precision = precision, scale = scale)
          ),
          value = Value.nullableDecimal(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class FloatFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String, val maxDigits: Int) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = FloatFieldPredicate(predicates)

    fun eq(value: Float) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = FloatType(maxDigits)),
          value = Value.float(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class NullableFloatFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String, val maxDigits: Int) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = NullableFloatFieldPredicate(predicates)

    fun eq(value: Float?) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = NullableFloatType(maxDigits)),
          value = Value.nullableFloat(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class IntFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = IntFieldPredicate(predicates)

    fun eq(value: Int) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = IntType(false)),
          value = Value.int(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class NullableIntFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = NullableIntFieldPredicate(predicates)

    fun eq(value: Int?) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = NullableIntType(false)),
          value = Value.nullableInt(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class LocalDateFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = LocalDateFieldPredicate(predicates)

    fun eq(value: LocalDate) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = LocalDateType),
          value = Value.date(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class NullableLocalDateFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = NullableLocalDateFieldPredicate(predicates)

    fun eq(value: LocalDate?) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = NullableLocalDateType),
          value = Value.nullableDate(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class LocalDateTimeFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = LocalDateTimeFieldPredicate(predicates)

    fun eq(value: LocalDateTime) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = LocalDateTimeType),
          value = Value.datetime(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class NullableLocalDateTimeFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = NullableLocalDateTimeFieldPredicate(predicates)

    fun eq(value: LocalDateTime?) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = NullableLocalDateTimeType),
          value = Value.nullableDatetime(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class TextFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = TextFieldPredicate(predicates)

    fun eq(value: String) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = StringType(null)),
          value = Value.text(value),
          operator = Operator.Equals,
        )
      )
  }
}

data class NullableTextFieldPredicate(val predicates: List<Predicate>) {
  @CriteriaDSL
  class Builder(val fieldName: String) {
    private val predicates: MutableList<Predicate> = mutableListOf()

    fun build() = NullableTextFieldPredicate(predicates)

    fun eq(value: String?) =
      predicates.add(
        Predicate(
          field = Field(name = fieldName, dataType = NullableStringType(null)),
          value = Value.nullableText(value),
          operator = Operator.Equals,
        )
      )
  }
}
