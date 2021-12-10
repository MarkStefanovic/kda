package kda.domain

@Suppress("MemberVisibilityCanBePrivate")
data class RowDiff(
  val srcRowCount: Int,
  val dstRowCount: Int,
  val added: Set<Row>,
  val deleted: Set<Row>,
  val updated: Set<Row>,
) {
  val absVariance: Int
    get() = extraRows + missingRows + staleRows

  val absVariancePct: Float
    get() = if ((srcRowCount == 0) && (dstRowCount == 0)) {
      0F
    } else if (srcRowCount == 0) {
      1F
    } else {
      absVariance / srcRowCount.toFloat()
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

  override fun toString(): String =
    """
      |RowDif [
      |  srcRowCount: $srcRowCount 
      |  dstRowCount: $dstRowCount 
      |  added: $added 
      |  deleted: $deleted 
      |  updated: $updated 
      |]
    """.trimMargin()
}

internal fun compareRows(
  dstRows: Set<Row>,
  srcRows: Set<Row>,
  primaryKeyFieldNames: Set<String>,
): RowDiff =
  if (srcRows.isEmpty() && dstRows.isEmpty()) {
    RowDiff(
      srcRowCount = 0,
      dstRowCount = 0,
      added = emptySet(),
      deleted = emptySet(),
      updated = emptySet(),
    )
  } else if (dstRows.isEmpty()) {
    RowDiff(
      srcRowCount = srcRows.count(),
      dstRowCount = 0,
      added = srcRows.toSet(),
      deleted = emptySet(),
      updated = emptySet(),
    )
  } else if (srcRows.isEmpty()) {
    RowDiff(
      srcRowCount = 0,
      dstRowCount = dstRows.count(),
      added = emptySet(),
      deleted = dstRows.toSet(),
      updated = emptySet(),
    )
  } else {
    val indexedSrcRows = srcRows.associate { row ->
      row.split(primaryKeyFieldNames)
    }

    val indexedDstRows = dstRows.associate { row ->
      row.split(primaryKeyFieldNames)
    }

    val addedRows: Set<Row> =
      indexedSrcRows
        .filterKeys { key ->
          indexedDstRows[key] == null
        }
        .map { (key, value) ->
          key.merge(value)
        }
        .toSet()

    val deletedRows: Set<Row> =
      indexedDstRows
        .filterKeys { key ->
          indexedSrcRows[key] == null
        }
        .map { (key, value) -> key.merge(value) }
        .toSet()

    val updatedRows: Set<Row> =
      indexedSrcRows
        .filter { (key, newRow) ->
          val oldRow = indexedDstRows[key]
          if (oldRow == null) {
            false
          } else {
            newRow.value != oldRow.value
          }
        }
        .map { (key, value) ->
          key.merge(value)
        }
        .toSet()

    RowDiff(
      srcRowCount = srcRows.count(),
      dstRowCount = dstRows.count(),
      added = addedRows,
      deleted = deletedRows,
      updated = updatedRows,
    )
  }
