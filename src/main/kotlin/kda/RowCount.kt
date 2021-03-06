package kda

import kda.adapter.selectAdapter
import kda.domain.DbDialect
import java.sql.Connection
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
fun getRowCount(
  con: Connection,
  dialect: DbDialect,
  schema: String?,
  table: String,
  showSQL: Boolean = false,
  queryTimeout: Duration = 30.minutes,
  timestampResolution: ChronoUnit = ChronoUnit.MILLIS,
): Int {
  val adapter = selectAdapter(
    dialect = dialect,
    con = con,
    showSQL = showSQL,
    queryTimeout = queryTimeout,
    timestampResolution = timestampResolution,
  )

  return adapter.rowCount(schema = schema, table = table)
}
