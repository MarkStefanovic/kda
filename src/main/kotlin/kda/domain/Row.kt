package kda.domain

@JvmInline
value class Row(val value: Map<String, Any?>) {
  val fields: Set<String>
    get() = value.keys

  val fieldsSorted: List<String>
    get() = value.keys.sorted()

  fun add(vararg values: Pair<String, Any?>): Row =
    Row(value.toMap(mutableMapOf(*values)))

  fun split(keyFieldNames: Set<String>): Pair<Row, Row> {
    val valueFieldNames = value.keys.subtract(keyFieldNames)

    val keyMap: Map<String, Any?> = value.filter { (field, _) ->
      keyFieldNames.contains(field)
    }

    val valueMap: Map<String, Any?> = value.filter { (field, _) ->
      valueFieldNames.contains(field)
    }

    return Row(keyMap) to Row(valueMap)
  }

  fun merge(other: Row): Row =
    Row(value + other.value)

  fun subset(fieldNames: Set<String>): Row =
    Row(value.filterKeys { it in fieldNames })

  companion object {
    fun of(vararg keyValuePair: Pair<String, Any?>): Row =
      Row(keyValuePair.toMap())
  }
}
