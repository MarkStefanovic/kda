package kda.domain

sealed class CompareRowsResult(
  open val srcSchema: String?,
  open val srcTable: String,
  open val destSchema: String?,
  open val destTable: String,
) {
  sealed class Error(
    override val srcSchema: String?,
    override val srcTable: String,
    override val destSchema: String?,
    override val destTable: String,
    open val errorMessage: String,
    open val originalError: Exception?,
  ) :
    CompareRowsResult(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
    ) {
    data class InspectTableFailed(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Exception?,
    ) :
      CompareRowsResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class Unexpected(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Exception,
    ) : CompareRowsResult.Error(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      errorMessage = errorMessage,
      originalError = originalError,
    )
  }

  data class Success(
    override val srcSchema: String?,
    override val srcTable: String,
    override val destSchema: String?,
    override val destTable: String,
    val srcRows: Int,
    val destRows: Int,
  ) : CompareRowsResult(
    srcSchema = srcSchema,
    srcTable = srcTable,
    destSchema = destSchema,
    destTable = destTable,
  ) {
    val pctDiff: Float
      get() = if (srcRows == 0) {
        0F
      } else {
        (destRows - srcRows) / srcRows.toFloat()
      }
  }
}
