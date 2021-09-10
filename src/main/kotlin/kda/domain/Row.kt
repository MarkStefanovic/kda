package kda.domain

val nonAlphaNum = "[^a-zA-Z0-9_]".toRegex()

@JvmInline
value class Row(private val row: Map<String, Value<*>>) {
  init {
    require(row.isNotEmpty())
    require(row.keys.all { fld -> fld.isNotEmpty() })
  }

  val fieldNames: Set<String>
    get() = row.keys

  fun value(fieldName: String): Value<*> {
    val stdFieldName = unwrapName(fieldName)
    return row[stdFieldName]
      ?: error(
        "A field named $stdFieldName was not found in the row.  " +
          "Available fields include the following: ${row.keys.joinToString(", ")}"
      )
  }

  fun subset(fieldNames: Set<String>): Row {
    val stdFieldNames = fieldNames.map(::unwrapName)
    return if (stdFieldNames.toSet() == row.keys) {
      this
    } else {
      require(stdFieldNames.all { fldName -> row.containsKey(fldName) })
      val subset = stdFieldNames.associateWith { fieldName -> row[fieldName]!! }
      Row(subset)
    }
  }

  companion object {
    fun of(vararg keyValuePairs: Pair<String, Value<*>>): Row = Row(keyValuePairs.toMap())
  }
}

private fun unwrapName(name: String) = name.replace(nonAlphaNum, "")
