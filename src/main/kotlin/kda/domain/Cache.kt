package kda.domain

interface Cache {
  fun addTable(schema: String?, table: Table)

  fun getTable(schema: String?, table: String): Table?
}
