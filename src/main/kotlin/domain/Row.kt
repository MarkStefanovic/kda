package domain

@JvmInline
value class Row(private val row: Map<String, Value<*>>) {
//    private val fieldNames: List<String>
//        get() = row.keys.sorted()

    val values: List<Value<*>>
        get() = row.keys.sorted().map { fldName -> row[fldName]!! }

//    fun value(fieldName: String): Value<*> =
//        row[fieldName] ?: error("A field named $fieldName was not found in the row.")

//    fun subset(vararg fieldNames: String): Row {
//        val subset = fieldNames.associate { fieldName ->
//            fieldName to value(fieldName)
//        }
//        return Row(subset)
//    }
}