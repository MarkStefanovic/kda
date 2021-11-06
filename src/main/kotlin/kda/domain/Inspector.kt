package kda.domain

interface Inspector {
  fun inspectTable(
    schema: String?,
    table: String,
    maxFloatDigits: Int,
    primaryKeyFieldNames: List<String>?,
  ): Table

  fun tableExists(
    schema: String?,
    table: String,
  ): Boolean
}
