package kda.domain

data class CopyTableResult(
  val srcTable: Table,
  val dstTable: Table,
) {
  override fun toString(): String =
    """
      |CopyTableResult [
      |  srcTable: $srcTable
      |  dstTable: $dstTable
      |]
    """.trimMargin()
}
