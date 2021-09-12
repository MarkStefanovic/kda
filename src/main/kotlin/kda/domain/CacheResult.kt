package kda.domain

sealed class CacheResult {
  sealed class AddTableDef(open val tableDef: Table) {
    data class Error(
      override val tableDef: Table,
      val originalError: Exception?,
      val errorMessage: String
    ) : AddTableDef(tableDef)

    data class Success(override val tableDef: Table) : AddTableDef(tableDef)
  }

  sealed class AddLatestTimestamp(
    open val schema: String,
    open val table: String,
    open val timestamps: Set<LatestTimestamp>,
  ) : CacheResult() {
    data class Error(
      override val schema: String,
      override val table: String,
      override val timestamps: Set<LatestTimestamp>,
      val originalError: Exception?,
      val errorMessage: String,
    ) : AddLatestTimestamp(
      schema = schema,
      table = table,
      timestamps = timestamps,
    )

    data class Success(
      override val schema: String,
      override val table: String,
      override val timestamps: Set<LatestTimestamp>,
    ) : AddLatestTimestamp(
      schema = schema,
      table = table,
      timestamps = timestamps,
    )
  }

  sealed class ClearTableDef(
    open val schema: String,
    open val table: String,
  ) : CacheResult() {
    data class Success(
      override val schema: String,
      override val table: String,
    ) : ClearTableDef(
      schema = schema,
      table = table
    )

    data class Error(
      override val schema: String,
      override val table: String,
      val originalError: Exception?,
      val errorMessage: String,
    ) : ClearTableDef(
      schema = schema,
      table = table
    )
  }

  sealed class ClearLatestTimestamps(
    open val schema: String,
    open val table: String,
  ) : CacheResult() {
    data class Success(
      override val schema: String,
      override val table: String,
    ) : ClearLatestTimestamps(
      schema = schema,
      table = table
    )

    data class Error(
      override val schema: String,
      override val table: String,
      val originalError: Exception?,
      val errorMessage: String,
    ) : ClearLatestTimestamps(
      schema = schema,
      table = table
    )
  }

  sealed class TableDef(
    open val schema: String,
    open val table: String,
  ) : CacheResult() {
    data class Success(
      override val schema: String,
      override val table: String,
      val tableDef: Table?,
    ) : TableDef(
      schema = schema,
      table = table
    )

    data class Error(
      override val schema: String,
      override val table: String,
      val originalError: Exception?,
      val errorMessage: String,
    ) : TableDef(
      schema = schema,
      table = table
    )
  }

  sealed class LatestTimestamps(
    open val schema: String,
    open val table: String,
  ) : CacheResult() {
    data class Success(
      override val schema: String,
      override val table: String,
      val timestamps: Set<LatestTimestamp>,
    ) : LatestTimestamps(
      schema = schema,
      table = table
    )

    data class Error(
      override val schema: String,
      override val table: String,
      val originalError: Exception?,
      val errorMessage: String,
    ) : LatestTimestamps(
      schema = schema,
      table = table
    )
  }
}
