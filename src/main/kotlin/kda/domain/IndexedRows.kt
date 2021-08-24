package kda.domain

class IndexedRows(private val rows: Map<Row, Row>) : Map<Row, Row> by rows {
    //    init {
    //        require(rows.count() > 0)
    //        require(rows.all { (keyRow, valueRow) ->
    // valueRow.sortedFieldNames.containsAll(keyRow.sortedFieldNames) })
    //    }

    //    val keys: Set<Row>
    //        get() = rows.keys
    //
//        val values: Set<Row>
//            get() = rows.values.toSet()
    //
    //    fun count(): Int = rows.values.count()
    //
    //    fun value(vararg pk: Pair<String, Value<*>>): Row? =
    //        rows[Row.of(*pk)]

    //    fun subset(vararg fieldNames: String): IndexedRows {
    //        return IndexedRows(rows.map { (key, row) -> key to row.subset(*fieldNames) }.toMap())
    //    }

    val fieldNames: Set<String>
        get() = rows.values.firstOrNull()?.fieldNames ?: setOf()

    companion object {
        fun empty() = IndexedRows(emptyMap())

        fun of(vararg keyValuePairs: Pair<Row, Row>) = IndexedRows(keyValuePairs.toMap())
    }
}

fun Collection<Row>.index(keyFields: Set<String>, includeFields: Set<String>): IndexedRows =
    IndexedRows(
        associateBy { row ->
            val subset = row.subset(fieldNames = includeFields)
            Row(keyFields.associateWith { fldName -> subset.value(fldName) })
        }
    )
