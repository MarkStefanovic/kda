package domain

data class RowDiff(
    val added: IndexedRows,
    val deleted: IndexedRows,
    val updated: IndexedRows,
    val includeFields: Set<String>,
    val compareFields: Set<String>,
)

fun compareRows(
    old: IndexedRows,
    new: IndexedRows,
    includeFields: Set<String>,
    compareFields: Set<String>,
): RowDiff {
    if (old.count() == 0)
        return RowDiff(
            added = new,
            deleted = IndexedRows(emptyMap()),
            updated = IndexedRows(emptyMap()),
            includeFields = includeFields,
            compareFields = compareFields,
        )

    if (new.count() == 0)
        return RowDiff(
            added = IndexedRows(emptyMap()),
            deleted = old,
            updated = IndexedRows(emptyMap()),
            includeFields = includeFields,
            compareFields = compareFields,
        )

    val oldRows =
        if (old.fieldNames == includeFields) old
        else old.mapValues { (_, value) -> value.subset(fieldNames = includeFields) }

    val newRows =
        if (new.fieldNames == includeFields) new
        else new.mapValues { (_, value) -> value.subset(fieldNames = includeFields) }

    val addedRows = new.filterKeys { key -> old[key] == null }

    val deletedRows = old.filterKeys { key -> new[key] == null }

    val updatedRows =
        newRows.filter { (key, value) ->
            val oldValue = oldRows[key]
            if (oldValue == null) false
            else value == oldValue
        }

    return RowDiff(
        added = IndexedRows(addedRows),
        deleted = IndexedRows(deletedRows),
        updated = IndexedRows(updatedRows),
        includeFields = includeFields,
        compareFields = compareFields,
    )
}
