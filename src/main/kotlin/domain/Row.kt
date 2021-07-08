package domain

@JvmInline
value class Row(private val row: Map<String, Value<*>>) {
    init {
        require(row.count() > 0)
        require(row.keys.all { fld -> fld.isNotEmpty() })
    }

    fun value(fieldName: String): Value<*> =
        row[fieldName] ?: error(
            "A field named $fieldName was not found in the row.  " +
                "Available fields include the following: ${row.keys.joinToString(", ")}"
        )

    fun subset(vararg fieldNames: String): Row {
        require(fieldNames.all { fldName -> row.containsKey(fldName) })
        val subset = fieldNames.sorted().associateWith { fieldName -> row[fieldName]!! }
        return Row(subset)
    }

    companion object {
        fun of(vararg keyValuePairs: Pair<String, Value<*>>): Row =
            Row(keyValuePairs.toMap())
    }
}