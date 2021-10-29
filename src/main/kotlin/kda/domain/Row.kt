package kda.domain

@JvmInline
value class Row(private val row: Map<String, Value<*>>) {
  init {
    require(row.isNotEmpty())
    require(row.keys.all { fld -> fld.isNotEmpty() })
  }

  val fieldNames: Set<String>
    get() = row.keys

  fun value(fieldName: String): Value<*> =
    row[fieldName] ?: throw KDAError.FieldNotFound(fieldName = fieldName, availableFieldNames = row.keys)

  fun subset(fieldNames: Set<String>): Row =
    if (fieldNames.toSet() == row.keys) {
      this
    } else {
      require(fieldNames.all { fldName -> row.containsKey(fldName) })
      val subset = fieldNames.associateWith { fieldName ->
        row[fieldName] ?: throw KDAError.FieldNotFound(fieldName = fieldName, availableFieldNames = row.keys)
      }
      Row(subset)
    }

  companion object {
    fun of(vararg keyValuePairs: Pair<String, Value<*>>): Row = Row(keyValuePairs.toMap())
  }
}
