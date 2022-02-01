package testutil

import kda.createCache
import kda.domain.DbDialect

fun sqliteCache() = createCache(
  dialect = DbDialect.SQLite,
  connector = { testSQLiteConnection() },
  schema = null,
)
