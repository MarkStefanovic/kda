package kda.domain

data class IndexedRows(private val rows: Map<Row, Row>) : Map<Row, Row> by rows {
  companion object {
    fun empty() = IndexedRows(emptyMap())

    fun of(vararg keyValuePairs: Pair<Row, Row>) = IndexedRows(keyValuePairs.toMap())
  }
}

fun Collection<Row>.index(keyFields: Set<String>, includeFields: Set<String>? = null): IndexedRows =
  IndexedRows(
    associateBy { row ->
      val subset = row.subset(fieldNames = includeFields ?: row.fieldNames)
      Row(keyFields.associateWith { fldName -> subset.value(fldName) })
    }
  )

fun Collection<Row>.index(keyField: String): IndexedRows =
  index(keyFields = setOf(keyField), includeFields = null)
