package kda.domain

import java.sql.Connection

data class Datasource(
    val connection: Connection,
    val executor: SQLExecutor,
    val adapter: SQLAdapter,
    val inspector: Inspector,
)