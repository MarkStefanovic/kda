package kda

import kda.adapter.Db
import kda.adapter.DbLatestTimestampRepository
import kda.adapter.DbTableDefRepository
import kda.adapter.SqliteDb
import kda.domain.CacheResult
import kda.domain.LatestTimestamp
import kda.domain.Table

interface Cache {
  fun addTableDef(tableDef: Table): CacheResult.AddTableDef

  fun addLatestTimestamp(
    schema: String,
    table: String,
    timestamps: Set<LatestTimestamp>,
  ): CacheResult.AddLatestTimestamp

  fun clearTableDef(schema: String, table: String): CacheResult.ClearTableDef

  fun clearLatestTimestamps(schema: String, table: String): CacheResult.ClearLatestTimestamps

  fun tableDef(schema: String, table: String): CacheResult.TableDef

  fun latestTimestamps(schema: String, table: String): CacheResult.LatestTimestamps
}

class DbCache(private val db: Db = SqliteDb) : Cache {
  private val latestTimestampRepo by lazy {
    DbLatestTimestampRepository(db)
  }

  private val tableDefRepo by lazy {
    DbTableDefRepository(db)
  }

  init {
    db.createTables()
  }

  override fun addTableDef(tableDef: Table) =
    try {
      tableDefRepo.add(tableDef)
      CacheResult.AddTableDef.Success(tableDef)
    } catch (e: Exception) {
      CacheResult.AddTableDef.Error(
        tableDef = tableDef,
        originalError = e,
        errorMessage = "An error occurred while adding the table def for " +
          "${tableDef.schema}.${tableDef.name}: ${e.message}."
      )
    }

  override fun addLatestTimestamp(
    schema: String,
    table: String,
    timestamps: Set<LatestTimestamp>,
  ) = try {
    timestamps.forEach { ts ->
      latestTimestampRepo.add(
        schema = schema,
        table = table,
        latestTimestamp = ts,
      )
    }
    CacheResult.AddLatestTimestamp.Success(
      schema = schema,
      table = table,
      timestamps = timestamps
    )
  } catch (e: Exception) {
    CacheResult.AddLatestTimestamp.Error(
      schema = schema,
      table = table,
      timestamps = timestamps,
      originalError = e,
      errorMessage = "An error occurred while adding the latest " +
        "timestamps, $timestamps: ${e.message}"
    )
  }

  override fun clearTableDef(schema: String, table: String) =
    try {
      tableDefRepo.delete(schema = schema, table = table)
      CacheResult.ClearTableDef.Success(
        schema = schema,
        table = table,
      )
    } catch (e: Exception) {
      CacheResult.ClearTableDef.Error(
        schema = schema,
        table = table,
        originalError = e,
        errorMessage = "An error occurred while clearing the table def " +
          "for $schema.$table: ${e.message}",
      )
    }

  override fun clearLatestTimestamps(schema: String, table: String) =
    try {
      latestTimestampRepo.delete(schema = schema, table = table)
      CacheResult.ClearLatestTimestamps.Success(
        schema = schema,
        table = table,
      )
    } catch (e: Exception) {
      CacheResult.ClearLatestTimestamps.Error(
        schema = schema,
        table = table,
        originalError = e,
        errorMessage = "An error occurred while clearing the timestamps " +
          "for $schema.$table: ${e.message}",
      )
    }

  override fun tableDef(schema: String, table: String) =
    try {
      val tableDef = tableDefRepo.get(schema = schema, table = table)
      CacheResult.TableDef.Success(
        schema = schema,
        table = table,
        tableDef = tableDef,
      )
    } catch (e: Exception) {
      CacheResult.TableDef.Error(
        schema = schema,
        table = table,
        originalError = e,
        errorMessage = "An error occurred while getting the table def " +
          "for $schema.$table: ${e.message}",
      )
    }

  override fun latestTimestamps(schema: String, table: String) =
    try {
      val timestamps = latestTimestampRepo.get(schema = schema, table = table)
      CacheResult.LatestTimestamps.Success(
        schema = schema,
        table = table,
        timestamps = timestamps,
      )
    } catch (e: Exception) {
      CacheResult.LatestTimestamps.Error(
        schema = schema,
        table = table,
        originalError = e,
        errorMessage = "An error occurred while getting the latest timestamps " +
          "for $schema.$table: ${e.message}",
      )
    }
}
