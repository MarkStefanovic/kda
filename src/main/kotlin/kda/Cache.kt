package kda

import kda.adapter.Db
import kda.adapter.DbLatestTimestampRepository
import kda.adapter.DbTableDefRepository
import kda.adapter.SqliteDb
import kda.domain.LatestTimestamp
import kda.domain.Table

interface Cache {
  fun addTableDef(tableDef: Table)

  fun addLatestTimestamp(
    schema: String,
    table: String,
    timestamps: Set<LatestTimestamp>,
  )

  fun clearTableDef(schema: String, table: String)

  fun clearLatestTimestamps(schema: String, table: String)

  fun tableDef(schema: String, table: String): Table?

  fun latestTimestamps(schema: String, table: String): Set<LatestTimestamp>
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

  override fun addTableDef(tableDef: Table) {
    tableDefRepo.add(tableDef)
  }

  override fun addLatestTimestamp(
    schema: String,
    table: String,
    timestamps: Set<LatestTimestamp>,
  ) {
    timestamps.forEach { ts ->
      latestTimestampRepo.add(
        schema = schema,
        table = table,
        latestTimestamp = ts,
      )
    }
  }

  override fun clearTableDef(schema: String, table: String) {
    tableDefRepo.delete(schema = schema, table = table)
  }

  override fun clearLatestTimestamps(schema: String, table: String) {
    latestTimestampRepo.delete(schema = schema, table = table)
  }

  override fun tableDef(schema: String, table: String) =
    tableDefRepo.get(schema = schema, table = table)

  override fun latestTimestamps(schema: String, table: String): Set<LatestTimestamp> =
    latestTimestampRepo.get(schema = schema, table = table)
}
