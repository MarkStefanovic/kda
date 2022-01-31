package kda.domain

data class DeltaResult(
  val added: Int,
  val deleted: Int,
  val updated: Int,
) {
  override fun toString() = """
    |DeltaResult [
    |  added: $added
    |  deleted: $deleted
    |  updated: $updated
    |]
  """.trimMargin()
}
