package kda.domain

interface TableDefRepository {
  fun add(table: Table)

  fun delete(schema: String, table: String)

  fun get(schema: String, table: String): Table?
}
