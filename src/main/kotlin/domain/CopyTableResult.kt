package domain

sealed class CopyTableResult(
  open val srcDialect: Dialect,
  open val destDialect: Dialect,
  open val srcSchema: String?,
  open val srcTable: String,
  open val destSchema: String?,
  open val destTable: String,
  open val includeFields: Set<String>,
  open val primaryKeyFields: List<String>,
) {
  sealed class Error(
    override val srcDialect: Dialect,
    override val destDialect: Dialect,
    override val srcSchema: String?,
    override val srcTable: String,
    override val destSchema: String?,
    override val destTable: String,
    override val includeFields: Set<String>,
    override val primaryKeyFields: List<String>,
    open val errorMessage: String,
    open val originalError: Exception,
  ) :
    CopyTableResult(
      srcDialect = srcDialect,
      destDialect = destDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      includeFields = includeFields,
      primaryKeyFields = primaryKeyFields,
    ) {
    data class CreateTableFailed(
      override val srcDialect: Dialect,
      override val destDialect: Dialect,
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val includeFields: Set<String>,
      override val primaryKeyFields: List<String>,
      override val errorMessage: String,
      override val originalError: Exception,
    ) :
      CopyTableResult.Error(
        srcDialect = srcDialect,
        destDialect = destDialect,
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        includeFields = includeFields,
        primaryKeyFields = primaryKeyFields,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class InspectTableFailed(
      override val srcDialect: Dialect,
      override val destDialect: Dialect,
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val includeFields: Set<String>,
      override val primaryKeyFields: List<String>,
      override val errorMessage: String,
      override val originalError: Exception,
    ) :
      CopyTableResult.Error(
        srcDialect = srcDialect,
        destDialect = destDialect,
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        includeFields = includeFields,
        primaryKeyFields = primaryKeyFields,
        errorMessage = errorMessage,
        originalError = originalError,
      )

    data class Unexpected(
      override val srcDialect: Dialect,
      override val destDialect: Dialect,
      override val srcSchema: String?,
      override val srcTable: String,
      override val destSchema: String?,
      override val destTable: String,
      override val includeFields: Set<String>,
      override val primaryKeyFields: List<String>,
      override val errorMessage: String,
      override val originalError: Exception,
    ) :
      CopyTableResult.Error(
        srcDialect = srcDialect,
        destDialect = destDialect,
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        includeFields = includeFields,
        primaryKeyFields = primaryKeyFields,
        errorMessage = errorMessage,
        originalError = originalError,
      )
  }

  data class Success(
    override val srcDialect: Dialect,
    override val destDialect: Dialect,
    override val srcSchema: String?,
    override val srcTable: String,
    override val destSchema: String?,
    override val destTable: String,
    override val includeFields: Set<String>,
    override val primaryKeyFields: List<String>,
    val srcTableDef: Table,
    val destTableDef: Table,
    val created: Boolean,
  ) :
    CopyTableResult(
      srcDialect = srcDialect,
      destDialect = destDialect,
      srcSchema = srcSchema,
      srcTable = srcTable,
      destSchema = destSchema,
      destTable = destTable,
      includeFields = includeFields,
      primaryKeyFields = primaryKeyFields,
    )
}
