package kda.domain

data class RowDiff(
  val added: IndexedRows,
  val deleted: IndexedRows,
  val updated: IndexedRows,
  val primaryKeyFields: Set<String>,
  val includeFields: Set<String>,
  val compareFields: Set<String>,
)

fun compareRows(
  old: Set<Row>,
  new: Set<Row>,
  primaryKeyFields: Set<String>,
  includeFields: Set<String>,
  compareFields: Set<String>,
): RowDiff {

  if (old.isEmpty())
    return RowDiff(
      added = new.index(keyFields = primaryKeyFields, includeFields = includeFields),
      deleted = IndexedRows.empty(),
      updated = IndexedRows.empty(),
      primaryKeyFields = primaryKeyFields,
      includeFields = includeFields,
      compareFields = compareFields,
    )

  if (new.isEmpty())
    return RowDiff(
      added = IndexedRows.empty(),
      deleted = old.index(keyFields = primaryKeyFields, includeFields = includeFields),
      updated = IndexedRows.empty(),
      primaryKeyFields = primaryKeyFields,
      includeFields = includeFields,
      compareFields = compareFields,
    )

  val oldRows: IndexedRows = old.index(keyFields = primaryKeyFields, includeFields = includeFields)

  val newRows: IndexedRows = new.index(keyFields = primaryKeyFields, includeFields = includeFields)

  val addedRows = newRows.filterKeys { key -> oldRows[key] == null }

  val deletedRows = oldRows.filterKeys { key -> newRows[key] == null }

  val updatedRows =
    newRows.filter { (key, value) ->
      val oldValue = oldRows[key]
      if (oldValue == null) false
      else value != oldValue
    }

  return RowDiff(
    added = IndexedRows(addedRows),
    deleted = IndexedRows(deletedRows),
    updated = IndexedRows(updatedRows),
    primaryKeyFields = primaryKeyFields,
    includeFields = includeFields,
    compareFields = compareFields,
  )
}
