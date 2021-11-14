package kda.domain

@JvmInline
value class Row(private val row: Map<String, Value<*>>) {
  init {
    require(row.isNotEmpty())
    require(row.keys.all { fld -> fld.isNotEmpty() })
  }

  val fieldNames: Set<String>
    get() = row.keys

  fun subset(fieldNames: Set<String>): Row =
    if (fieldNames.toSet() == row.keys) {
      this
    } else {
      fieldNames.forEach { fldName ->
        require(row.containsKey(fldName)) {
          "The row ${row.toMap()} does not contain the field, $fldName."
        }
      }
      val subset = fieldNames.associateWith { fieldName ->
        row[fieldName] ?: throw KDAError.FieldNotFound(fieldName = fieldName, availableFieldNames = row.keys)
      }
      Row(subset)
    }

  fun toMap(): Map<String, Any?> =
    row.entries.associate { entry -> entry.key to entry.value.value }

  fun value(fieldName: String): Value<*> =
    row[fieldName] ?: throw KDAError.FieldNotFound(fieldName = fieldName, availableFieldNames = row.keys)

  companion object {
    fun of(vararg keyValuePairs: Pair<String, Value<*>>): Row = Row(keyValuePairs.toMap())
  }
}

fun Set<Row>.distinctOnPK(pkCols: Set<String>): Set<Row> =
  associateBy { row ->
    pkCols.map { pk ->
      row.value(pk)
    }.toSet()
  }.values.toSet()
