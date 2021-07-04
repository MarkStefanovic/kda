package domain

interface SQLAdapter {
    fun createTable(table: Table): String

    fun dropTable(schemaName: String, tableName: String): String

    fun add(table: Table, rows: Rows): String

    fun delete(table: Table, primaryKeyValues: Rows): String

    fun update(table: Table, rows: Rows): String

    fun select(table: Table, primaryKeyValues: Rows): String
}

class StandardSQLAdapter(private val impl: SQLAdapterImplDetails): SQLAdapter {
    override fun createTable(table: Table): String {
        val fullTableName = "${impl.wrapName(table.schema)}.${impl.wrapName(table.schema)}"
        val colDefCSV = table.fields.joinToString(", ") { fld -> impl.fieldDef(fld) }
        val pkCSV = table.sortedPrimaryKeyFieldNames.joinToString(", ") { fld -> impl.wrapName(fld) }
        return "CREATE TABLE $fullTableName ($colDefCSV, PRIMARY KEY ($pkCSV))"
    }

    override fun dropTable(schemaName: String, tableName: String): String {
        val fullTableName = "${impl.wrapName(schemaName)}.${impl.wrapName(tableName)}"
        return "DROP TABLE $fullTableName"
    }

    override fun add(table: Table, rows: Rows): String {
        val fullTableName = "${impl.wrapName(table.schema)}.${impl.wrapName(table.schema)}"
        val fieldNameCSV = table.sortedFieldNames.joinToString(", ") { fldName -> impl.wrapName(fldName) }
        val valuesCSV = impl.valuesExpression(rows)
        return "INSERT INTO $fullTableName ($fieldNameCSV) VALUES ($valuesCSV)"
    }

    override fun delete(table: Table, primaryKeyValues: Rows): String {
        require (table.sortedPrimaryKeyFieldNames.count() > 0)

        val fullTableName = "${impl.wrapName(table.schema)}.${impl.wrapName(table.schema)}"
        return if (table.sortedPrimaryKeyFieldNames.count() > 1) {

            "WITH d ($pkColCSV) AS ($pkValCSV) DELETE FROM $fullTableName t USING d WHERE $whereClause"
        } else {
            val pkCol = impl.wrapName(table.sortedPrimaryKeyFieldNames.first())
            val valuesCSV = primaryKeyValues.values.flatMap { row -> row.values.map { value -> impl.wrapValue(value) } }.joinToString(", ")
            "DELETE FROM $fullTableName WHERE $pkCol IN ($valuesCSV)"
        }
    }

    override fun update(table: Table, rows: Rows): String {
        TODO("Not yet implemented")
    }

    override fun select(table: Table, primaryKeyValues: Rows): String {
        TODO("Not yet implemented")
    }
}

