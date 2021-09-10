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

  fun value(fieldName: String): Value<*> =
    row[unwrapName(fieldName)]
      ?: error(
        "A field named ${unwrapName(fieldName)} was not found in the row.  " +
          "Available fields include the following: ${row.keys.joinToString(", ")}"
      )

  fun subset(fieldNames: Set<String>): Row {
    return if (fieldNames.toSet() == row.keys) {
      this
    } else {
      require(fieldNames.all { fldName -> row.containsKey(fldName) })
      val subset = fieldNames.associateWith { fieldName -> row[fieldName]!! }
      Row(subset)
    }
  }

  companion object {
    fun of(vararg keyValuePairs: Pair<String, Value<*>>): Row = Row(keyValuePairs.toMap())
  }
}

private fun unwrapName(name: String) = name.replace(nonAlphaNum, "")
