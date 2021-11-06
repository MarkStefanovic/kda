package kda.testutil

import kda.Cache
import kda.domain.LatestTimestamp
import kda.domain.Table

class DummyCache : Cache {
  override fun addTableDef(tableDef: Table) = Result.success(Unit)

  override fun addLatestTimestamp(schema: String?, table: String, timestamps: Set<LatestTimestamp>) = Result.success(Unit)

  override fun clearTableDef(schema: String?, table: String) = Result.success(Unit)

  override fun clearLatestTimestamps(schema: String?, table: String) = Result.success(Unit)

  override fun tableDef(schema: String?, table: String): Result<Table?> = Result.success(null)

  override fun latestTimestamps(schema: String?, table: String) = Result.success(setOf<LatestTimestamp>())
}
