package kda

import kda.adapter.Db
import kda.adapter.DbLatestTimestampRepository
import kda.adapter.DbTableDefRepository
import kda.adapter.SQLDb
import kda.domain.LatestTimestamp
import kda.domain.Table
import org.jetbrains.exposed.sql.Database

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

class DbCache(private val db: Db) : Cache {
  private val latestTimestampRepo by lazy {
    DbLatestTimestampRepository()
  }

  private val tableDefRepo by lazy {
    DbTableDefRepository()
  }

  init {
    db.createTables()
  }

  override fun addTableDef(tableDef: Table): Result<Unit> = runCatching {
    db.exec {
      tableDefRepo.add(tableDef)
    }
  }

  override fun addLatestTimestamp(
    schema: String?,
    table: String,
    timestamps: Set<LatestTimestamp>,
  ) = runCatching {
    db.exec {
      timestamps.forEach { ts ->
        latestTimestampRepo.add(
          schema = schema,
          table = table,
          latestTimestamp = ts,
        )
      }
    }
  }

  override fun clearTableDef(schema: String?, table: String) = runCatching {
    db.exec {
      tableDefRepo.delete(schema = schema, table = table)
    }
  }

  override fun clearLatestTimestamps(schema: String?, table: String) = runCatching {
    db.exec {
      latestTimestampRepo.delete(schema = schema, table = table)
    }
  }

  override fun tableDef(schema: String?, table: String) = runCatching {
    db.fetch {
      tableDefRepo.get(schema = schema, table = table)
    }
  }

  override fun latestTimestamps(schema: String?, table: String) = runCatching {
    db.fetch {
      latestTimestampRepo.get(schema = schema, table = table)
    }
  }
}

val sqliteCache: Cache by lazy {
  DbCache(SQLDb(Database.connect(url = "jdbc:sqlite:file:test?mode=memory&cache=shared", driver = "org.sqlite.JDBC")))
}
