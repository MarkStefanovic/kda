package service

import adapter.pg.pgDatasource
import domain.*
import java.sql.Connection

fun sync(
    srcCon: Connection,
    destCon: Connection,
    srcDialect: Dialect,
    destDialect: Dialect,
    srcSchema: String?,
    srcTable: String,
    destSchema: String?,
    destTable: String,
    compareFields: Set<String>? = null,
    pkFields: List<String>? = null,
    includeFields: Set<String>? = null,
    maxFloatDigits: Int = 5,
): SyncResult {
    if (compareFields != null) require(compareFields.count() > 0) {
        "If compareFields is provided, then it must contain at least one field name."
    }

    if (pkFields != null) require(pkFields.count() > 0) {
        "If pkFields is provided, then it must contain at least one field name."
    }

    if (includeFields != null) require(includeFields.count() > 0) {
        "If includeFields is provided, then it must contain at least one field name."
    }

    val src = when (srcDialect) {
        Dialect.HortonworksHive -> TODO()
        Dialect.PostgreSQL      -> pgDatasource(con = srcCon)
    }

    val dest = when (destDialect) {
        Dialect.HortonworksHive -> TODO()
        Dialect.PostgreSQL      -> pgDatasource(con = destCon)
    }

    var srcTableDef = src.inspector.inspectTable(schema = srcSchema, table = srcTable, maxFloatDigits = maxFloatDigits)

    val pkFieldsFinal: List<String> = if (pkFields == null) {
        srcTableDef.primaryKeyFieldNames
    } else {
        require(pkFields.all { fldName -> srcTableDef.sortedFieldNames.contains(fldName) }) {
            val missingFields: Set<String> = srcTableDef.sortedFieldNames.toSet().minus(pkFields.toSet())
            "The following primary key field(s) were specified: ${pkFields.joinToString(", ")}.  " + "However, the table does not include the following fields: ${
                missingFields.joinToString(", ")
            }.  " + "The table includes the following fields: ${srcTableDef.sortedFieldNames.joinToString(", ")}"
        }
        pkFields
    }

    val fieldsFinal: Set<Field> = if (includeFields == null) {
        srcTableDef.fields
    } else {
        require(includeFields.all { fldName -> srcTableDef.sortedFieldNames.contains(fldName) }) {
            val missingFields: Set<String> = includeFields.minus(pkFieldsFinal.toSet())
            "The includeFields specified, ${includeFields.joinToString(", ")}, does not include the " + "following primary-key fields: ${
                missingFields.joinToString(", ")
            }."
        }
        srcTableDef.fields.filter { fld -> fld.name in includeFields }.toSet()
    }

    val srcTableDefFinal = srcTableDef.copy(
        fields = fieldsFinal,
        primaryKeyFieldNames = pkFieldsFinal,
    )

    val destTableDefFinal = srcTableDefFinal.copy(
        schema = destSchema,
        name = destTable,
    )

    val compareFieldNamesFinal: Set<String> = if (compareFields == null) {
        srcTableDefFinal.sortedFieldNames.toSet().minus(srcTableDefFinal.primaryKeyFieldNames)
    } else {
        srcTableDefFinal.sortedFieldNames.filter { fldName -> fldName in compareFields }.toSet()
    }

    val lkpTableFieldNames = pkFieldsFinal.union(compareFieldNamesFinal)

    val srcLkpTable = srcTableDefFinal.subset(fieldNames = lkpTableFieldNames)
    val srcKeysSQL: String =
        src.adapter.select(table = srcLkpTable)
    val srcKeys: IndexedRows = src.executor.fetchRows(sql = srcKeysSQL, fields = srcTableDefFinal.fields)
        .index(pkFieldsFinal.toSet())

    val destLkpTable = destTableDefFinal.subset(fieldNames = lkpTableFieldNames)
    val destKeysSQL: String =
        dest.adapter.select(table = destLkpTable)
    val destKeys: IndexedRows = dest.executor.fetchRows(sql = destKeysSQL, fields = srcTableDefFinal.fields)
        .index(pkFieldsFinal.toSet())

    val rowDiff: RowDiff = compareRows(
        old = destKeys,
        new = srcKeys,
        includeFields = lkpTableFieldNames,
        compareFields = compareFieldNamesFinal,
    )

    if (rowDiff.added.keys.count() > 0) {
        val addedRowsSQL: String = src.adapter.selectKeys(table = srcTableDefFinal, primaryKeyValues = rowDiff.added)
        val addedRows: List<Row> = src.executor.fetchRows(sql = addedRowsSQL)
        val insertSQL: String = src.adapter.add(table = destTableDefFinal, rows = addedRows)
        src.executor.execute(sql = insertSQL)
    }

//    if (pkFields != null) {
//        require(pkFields.all { fldName -> srcTableDef.sortedFieldNames.contains(fldName) }) {
//            val missingFields: Set<String> = srcTableDef.sortedFieldNames.toSet().minus(pkFields)
//            "The primary key field(s) were specified, ${pkFields.joinToString(", ")}.  " +
//                "However, the table does not include the following fields: ${missingFields.joinToString(", ")}.  " +
//                "Available fields include the following: ${srcTableDef.sortedFieldNames.joinToString(", ")}"
//        }
//        srcTableDef = srcTableDef.copy(primaryKeyFields = pkFields.toList())
//    }

    if (includeFields != null) {
        srcTableDef = srcTableDef.copy(fields = srcTableDef.fields.filter { fld -> fld.name in includeFields }.toSet())
    }

    val defaultResult = SyncResult.success(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        added = 0,
        deleted = 0,
        updated = 0,
    )

    if (includeFields != null) srcTableDef = srcTableDef.subset(fieldNames = includeFields)

    val destTableDef = srcTableDef.copy(schema = destSchema, name = destTable)

    if (!dest.inspector.tableExists(schema = destSchema, table = destTable)) {
        val createTableSQL = dest.adapter.createTable(destTableDef)
        dest.executor.execute(createTableSQL)
    }

    return result
}
