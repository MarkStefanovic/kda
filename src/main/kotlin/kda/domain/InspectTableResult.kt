package kda.domain

sealed class InspectTableResult(
  open val schema: String?,
  open val table: String,
  open val dialect: Dialect,
  open val primaryKeyFieldNames: List<String>,
) {
  sealed class Error(
    override val schema: String?,
    override val table: String,
    override val dialect: Dialect,
    override val primaryKeyFieldNames: List<String>,
    open val errorMessage: String,
    open val originalError: Exception?
  ) : InspectTableResult(
    schema = schema,
    table = table,
    dialect = dialect,
    primaryKeyFieldNames = primaryKeyFieldNames,
  ) {
    data class TableDoesNotExist(
      override val schema: String?,
      override val table: String,
      override val dialect: Dialect,
      override val primaryKeyFieldNames: List<String>,
    ) : InspectTableResult.Error(
      schema = schema,
      table = table,
      dialect = dialect,
      primaryKeyFieldNames = primaryKeyFieldNames,
      errorMessage = "$schema.$table does not exist.",
      originalError = null,
    )

    data class InvalidArgument(
      override val schema: String?,
      override val table: String,
      override val dialect: Dialect,
      override val primaryKeyFieldNames: List<String>,
      override val errorMessage: String,
      override val originalError: Exception?,
      val argumentName: String,
      val argumentValue: Any?,
    ) : InspectTableResult.Error(
      schema = schema,
      table = table,
      dialect = dialect,
      primaryKeyFieldNames = primaryKeyFieldNames,
      errorMessage = errorMessage,
      originalError = originalError,
    )

    data class InspectTableFailed(
      override val schema: String?,
      override val table: String,
      override val dialect: Dialect,
      override val primaryKeyFieldNames: List<String>,
      override val errorMessage: String,
      override val originalError: Exception?,
    ) : InspectTableResult.Error(
      schema = schema,
      table = table,
      dialect = dialect,
      primaryKeyFieldNames = primaryKeyFieldNames,
      errorMessage = errorMessage,
      originalError = originalError,
    )

    data class Unexpected(
      override val schema: String?,
      override val table: String,
      override val dialect: Dialect,
      override val primaryKeyFieldNames: List<String>,
      override val errorMessage: String,
      override val originalError: Exception?,
    ) : InspectTableResult.Error(
      schema = schema,
      table = table,
      dialect = dialect,
      primaryKeyFieldNames = primaryKeyFieldNames,
      errorMessage = errorMessage,
      originalError = originalError,
    )
  }

  data class Success(
    override val schema: String?,
    override val table: String,
    override val dialect: Dialect,
    override val primaryKeyFieldNames: List<String>,
    val tableDef: Table,
  ) : InspectTableResult(
    schema = schema,
    table = table,
    dialect = dialect,
    primaryKeyFieldNames = primaryKeyFieldNames,
  )
}
