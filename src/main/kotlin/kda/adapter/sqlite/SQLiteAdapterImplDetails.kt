package kda.adapter.sqlite

import kda.adapter.std.StdSQLAdapterImplDetails
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SQLiteAdapterImplDetails : StdSQLAdapterImplDetails() {
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")

  override fun wrapLocalDateValue(value: LocalDate?): String =
    if (value == null) "DATE(NULL)" else "DATE('${value.format(dateFormatter)}')"

  override fun wrapLocalDateTimeValue(value: LocalDateTime?): String =
    if (value == null) "DATETIME(NULL)" else "DATETIME('${value.format(dateTimeFormatter)}')"
}
