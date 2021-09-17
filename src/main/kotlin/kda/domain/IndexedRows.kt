package kda.domain

data class IndexedRows(private val rows: Map<Row, Row>) : Map<Row, Row> by rows {
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
