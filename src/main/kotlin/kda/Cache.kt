package kda

import kda.adapter.DbLatestTimestampRepository
import kda.adapter.DbTableDefRepository
import kda.adapter.SQLDb
import kda.domain.LatestTimestamp
import kda.domain.Table
import org.jetbrains.exposed.sql.Database
import java.sql.DriverManager

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
  private val exposedDb: Database,
  private val logToConsole: Boolean,
) : Cache {

  private val db by lazy {
    SQLDb(exposedDb = exposedDb, logToConsole = logToConsole)
  }

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
  val url = "jdbc:sqlite:file:test?mode=memory&cache=shared"
  DriverManager.getConnection(url) // needed to keep in-memory database alive
  DbCache(
    exposedDb = Database.connect(url = url, driver = "org.sqlite.JDBC"),
    logToConsole = false,
  )
}
