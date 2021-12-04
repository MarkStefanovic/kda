package kda.domain

data class SyncResult(
  val deleted: Int,
  val upserted: Int,
) {
  override fun toString(): String =
    """
      |SyncResult [
      |  deleted: $deleted
      |  upserterd: $upserted
      |]
    """.trimMargin()
}
