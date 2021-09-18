package kda.domain

import java.sql.Connection

data class Datasource(
  internal val connection: Connection,
  internal val executor: SQLExecutor,
  internal val adapter: SQLAdapter,
  internal val inspector: Inspector,
)
