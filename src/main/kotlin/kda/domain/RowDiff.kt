package kda.domain

data class RowDiff(
  val srcRows: IndexedRows,
  val destRows: IndexedRows,
  val added: IndexedRows,
  val deleted: IndexedRows,
  val updated: IndexedRows,
  val primaryKeyFields: Set<String>,
  val includeFields: Set<String>,
  val compareFields: Set<String>,
) {
  val absVariance: Int
    get() = extraRows + missingRows + staleRows

  val absVariancePct: Float
    get() = if ((srcRows.isEmpty()) && (destRows.isEmpty())) {
      0F
    } else if (srcRows.isEmpty()) {
      1F
    } else {
      absVariance / srcRows.count().toFloat()
    }

  val description =
    "- " + listOf(
      "extra rows: $extraRows",
      "missing rows: $missingRows",
      "stale rows: $staleRows"
    ).joinToString("\n- ")

  val extraRows: Int
    get() = deleted.count()

  val missingRows: Int
    get() = added.count()

  val staleRows: Int
    get() = updated.count()
}

fun compareRows(
  dest: Set<Row>,
  src: Set<Row>,
  primaryKeyFields: Set<String>,
  includeFields: Set<String>,
  compareFields: Set<String>,
): RowDiff = if (dest.isEmpty()) {
  val srcRows = src.index(keyFields = primaryKeyFields, includeFields = includeFields)
  RowDiff(
    srcRows = srcRows,
    destRows = IndexedRows.empty(),
    added = srcRows,
    deleted = IndexedRows.empty(),
    updated = IndexedRows.empty(),
    primaryKeyFields = primaryKeyFields,
    includeFields = includeFields,
    compareFields = compareFields,
  )
} else if (src.isEmpty()) {
  val destRows = dest.index(keyFields = primaryKeyFields, includeFields = includeFields)
  RowDiff(
    srcRows = IndexedRows.empty(),
    destRows = destRows,
    added = IndexedRows.empty(),
    deleted = destRows,
    updated = IndexedRows.empty(),
    primaryKeyFields = primaryKeyFields,
    includeFields = includeFields,
    compareFields = compareFields,
  )
} else {
  val destRows = dest.index(keyFields = primaryKeyFields, includeFields = includeFields)
  val srcRows = src.index(keyFields = primaryKeyFields, includeFields = includeFields)
  val addedRows = srcRows.filterKeys { key -> destRows[key] == null }
  val deletedRows = destRows.filterKeys { key -> srcRows[key] == null }
  val updatedRows = srcRows.filter { (key, value) ->
    val oldValue = destRows[key]
    if (oldValue == null) false
    else value != oldValue
  }
  RowDiff(
    srcRows = srcRows,
    destRows = destRows,
    added = IndexedRows(addedRows),
    deleted = IndexedRows(deletedRows),
    updated = IndexedRows(updatedRows),
    primaryKeyFields = primaryKeyFields,
    includeFields = includeFields,
    compareFields = compareFields,
  )
}
