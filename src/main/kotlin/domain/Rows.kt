package domain

@JvmInline
value class Rows(private val rows: Map<Row, Row>) {
//    val fieldNames: List<String>
//        get() = if (values.isEmpty()) emptyList() else values.first().fieldNames
//    val keys: List<Row>
//        get() = rows.keys.toList()

    val values: List<Row>
        get() = rows.values.toList()

//    fun subset(vararg fieldNames: String): Rows {
//        return Rows(rows.map { (key, row) -> key to row.subset(*fieldNames) }.toMap())
//    }
}