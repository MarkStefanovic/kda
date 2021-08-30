package kda.domain

interface SQLAdapter {
  fun createTable(table: Table): String

  fun dropTable(schema: String?, table: String): String

  fun add(table: Table, rows: Set<Row>): String

  fun deleteKeys(table: Table, primaryKeyValues: Set<Row>): String

  fun update(table: Table, rows: Set<Row>): String

  fun select(table: Table, criteria: List<Criteria>): String

  fun selectKeys(table: Table, primaryKeyValues: Set<Row>): String
}
