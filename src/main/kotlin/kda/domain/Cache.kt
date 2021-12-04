package kda.domain

import java.time.LocalDateTime

interface Cache {
  fun addTable(schema: String?, table: Table)

  fun addTimestamp(schema: String?, table: String, fieldName: String, ts: LocalDateTime?)

  fun getTable(schema: String?, table: String): Table?

  fun getTimestamp(schema: String?, table: String, fieldName: String): LocalDateTime?
}
