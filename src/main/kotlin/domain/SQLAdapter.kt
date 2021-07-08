package domain

interface SQLAdapter {
    fun createTable(table: Table): String

    fun dropTable(schema: String?, table: String): String

    fun add(table: Table, rows: Rows): String

    fun delete(table: Table, primaryKeyValues: Rows): String

    fun update(table: Table, rows: Rows): String

    fun select(table: Table, primaryKeyValues: Rows): String
}
