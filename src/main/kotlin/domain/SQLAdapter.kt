package domain

interface SQLAdapter {
    fun createTable(table: Table): String

    fun dropTable(schema: String?, table: String): String

    fun add(table: Table, rows: IndexedRows): String

    fun delete(table: Table, primaryKeyValues: IndexedRows): String

    fun update(table: Table, rows: IndexedRows): String

    fun select(table: Table, primaryKeyValues: IndexedRows): String
}
