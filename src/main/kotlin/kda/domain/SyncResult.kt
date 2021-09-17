package kda.domain

data class SyncResult(
  val srcTableDef: Table,
  val destTableDef: Table,
  val added: IndexedRows,
  val deleted: IndexedRows,
  val updated: IndexedRows,
)
