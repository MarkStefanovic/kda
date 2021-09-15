package kda.domain

interface SQLAdapter {
  fun add(table: Table, rows: Set<Row>): String

  fun createTable(table: Table): String

  fun deleteKeys(table: Table, primaryKeyValues: Set<Row>): String

  fun dropTable(schema: String?, table: String): String

  fun getRowCount(schema: String?, table: String): String

  fun merge(table: Table, rows: Set<Row>): String

  fun select(table: Table, criteria: Set<Criteria>): String

  fun selectKeys(table: Table, primaryKeyValues: Set<Row>): String

  fun selectMaxValues(table: Table, fieldNames: Set<String>): String

  fun update(table: Table, rows: Set<Row>): String
}
