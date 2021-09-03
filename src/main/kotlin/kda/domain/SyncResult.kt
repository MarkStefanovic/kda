package kda.domain

sealed class SyncResult(
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
    SyncResult(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
    ) {

    data class AddRowsFailed(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
      val rows: Set<Row>,
    ) :
      SyncResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class CopyTableFailed(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
      val srcTableDef: Table,
    ) :
      SyncResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class DeleteRowsFailed(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
      val rows: Set<Row>,
    ) :
      SyncResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

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
      SyncResult.Error(
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
      SyncResult.Error(
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
      SyncResult.Error(
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
      override val originalError: Throwable?,
    ) :
      SyncResult.Error(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class UpdateRowsFailed(
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val errorMessage: String,
      override val originalError: Throwable?,
      val rows: Set<Row>,
    ) :
      SyncResult.Error(
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
    val added: Int,
    val deleted: Int,
    val updated: Int,
  ) :
    SyncResult(
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
    )
}
