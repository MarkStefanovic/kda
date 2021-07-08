package domain

@JvmInline
value class Rows(private val rows: Map<Row, Row>) {
    init {
        require(rows.count() > 0)
//        require(rows.all { (keyRow, valueRow) -> valueRow.sortedFieldNames.containsAll(keyRow.sortedFieldNames) })
    }

    val values: List<Row>
        get() = rows.values.toList()

    fun count(): Int = rows.values.count()

    fun subset(vararg fieldNames: String): Rows {
        return Rows(rows.map { (key, row) -> key to row.subset(*fieldNames) }.toMap())
    }

    companion object {
        fun of(vararg keyValuePairs: Pair<Row, Row>) =
            Rows(keyValuePairs.toMap())
    }
}