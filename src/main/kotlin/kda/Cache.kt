package kda

import kda.adapter.DbLatestTimestampRepository
import kda.adapter.DbTableDefRepository
import kda.adapter.createTables
import kda.domain.Datasource
import kda.domain.LatestTimestamp
import kda.domain.Table

interface Cache {
  fun addTableDef(tableDef: Table): Result<Unit>

  fun addLatestTimestamp(
    schema: String?,
    table: String,
    timestamps: Set<LatestTimestamp>,
  ): Result<Unit>

  fun clearTableDef(schema: String?, table: String): Result<Unit>

  fun clearLatestTimestamps(schema: String?, table: String): Result<Unit>

  fun tableDef(schema: String?, table: String): Result<Table?>

  fun latestTimestamps(schema: String?, table: String): Result<Set<LatestTimestamp>>
}

class DbCache(
  private val ds: Datasource,
  private val showSQL: Boolean,
  private val maxFloatDigits: Int = 5,
) : Cache {

  private val latestTimestampRepo by lazy {
    DbLatestTimestampRepository(
      ds = ds,
      showSQL = showSQL,
    )
  }

  private val tableDefRepo by lazy {
    DbTableDefRepository(
      ds = ds,
      showSQL = showSQL,
      maxFloatDigits = maxFloatDigits,
    )
  }

  init {
    createTables(ds = ds, showSQL = showSQL)
  }

  override fun addTableDef(tableDef: Table): Result<Unit> = runCatching {
    tableDefRepo.add(tableDef)
  }

  override fun addLatestTimestamp(
    schema: String?,
    table: String,
    timestamps: Set<LatestTimestamp>,
  ) = runCatching {
    timestamps.forEach { ts ->
      latestTimestampRepo.add(
        schema = schema,
        table = table,
        latestTimestamp = ts,
      )
    }
  }

  override fun clearTableDef(schema: String?, table: String) = runCatching {
    tableDefRepo.delete(schema = schema, table = table)
  }

  override fun clearLatestTimestamps(schema: String?, table: String) = runCatching {
    latestTimestampRepo.delete(schema = schema, table = table)
  }

  override fun tableDef(schema: String?, table: String) = runCatching {
    tableDefRepo.get(schema = schema, table = table)
  }

  override fun latestTimestamps(schema: String?, table: String) = runCatching {
    latestTimestampRepo.get(schema = schema, table = table)
  }
}
