package domain

class IndexedRows(private val rows: Map<Row, Row>) : Map<Row, Row> by rows {
    //    init {
    //        require(rows.count() > 0)
    //        require(rows.all { (keyRow, valueRow) ->
    // valueRow.sortedFieldNames.containsAll(keyRow.sortedFieldNames) })
    //    }

    //    val keys: Set<Row>
    //        get() = rows.keys
    //
    //    val values: List<Row>
    //        get() = rows.values.toList()
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
        fun of(vararg keyValuePairs: Pair<Row, Row>) = IndexedRows(keyValuePairs.toMap())
    }
}

fun List<Row>.index(keyFields: Set<Field>): IndexedRows =
    IndexedRows(
        associateBy { row ->
            Row(keyFields.associate { fld -> fld.name to row.value(fld.name) })
        }
    )
