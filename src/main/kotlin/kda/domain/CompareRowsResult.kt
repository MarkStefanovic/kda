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
    open val originalError: Throwable?
  ) :
    CompareRowsResult(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
    ) {
    data class InvalidArgument(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
      val argumentName: String,
      val argumentValue: Any?,
    ) :
      CompareRowsResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class InspectTableFailed(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
    ) :
      CompareRowsResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class RowComparisonFailed(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
      val srcRows: Set<Row>,
      val destRows: Set<Row>,
      val pkFields: Set<String>,
      val compareFields: Set<String>,
      val includeFields: Set<String>,
    ) :
      CompareRowsResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class SourceTableDoesNotExist(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
    ) :
      CompareRowsResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "$srcSchema.$srcTable does not exist.",
        originalError = null,
      )

    data class DestTableDoesNotExist(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
    ) :
      CompareRowsResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = "$destSchema.$destTable does not exist.",
        originalError = null,
      )

    data class Unexpected(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
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
