package kda.domain

data class Field(val name: String, val dataType: DataType<*>) {
  fun wrapValue(value: Any?) = dataType.wrapValue(value)

  val nullable: Boolean
    get() = when (dataType) {
      DataType.bool -> false
      is DataType.decimal -> false
      is DataType.float -> false
      is DataType.int -> false
      DataType.localDate -> false
      DataType.localDateTime -> false
      is DataType.text -> false
      DataType.nullableBool -> true
      is DataType.nullableDecimal -> true
      is DataType.nullableFloat -> true
      is DataType.nullableInt -> true
      DataType.nullableLocalDate -> true
      DataType.nullableLocalDateTime -> true
      is DataType.nullableText -> true
    }
}
