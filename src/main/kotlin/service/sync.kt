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
    primaryKeyFieldNames: List<String>? = null,
    includeFields: Set<String>? = null,
    maxFloatDigits: Int = 5,
): SyncResult {
    if (compareFields != null)
        require(compareFields.count() > 0) {
            "If compareFields is provided, then it must contain at least one field name."
        }

    if (primaryKeyFieldNames != null)
        require(primaryKeyFieldNames.count() > 0) {
            "If pkFields is provided, then it must contain at least one field name."
        }

    if (includeFields != null)
        require(includeFields.count() > 0) {
            "If includeFields is provided, then it must contain at least one field name."
        }

    val src: Datasource =
        when (srcDialect) {
            Dialect.HortonworksHive -> TODO()
            Dialect.PostgreSQL -> pgDatasource(con = srcCon)
        }

    val dest: Datasource =
        when (destDialect) {
            Dialect.HortonworksHive -> TODO()
            Dialect.PostgreSQL -> pgDatasource(con = destCon)
        }

    val srcTableDef: Table =
        src.inspector.inspectTable(
            schema = srcSchema,
            table = srcTable,
            maxFloatDigits = maxFloatDigits
        )

    val pkFieldsFinal: List<String> =
        if (primaryKeyFieldNames == null) {
            srcTableDef.primaryKeyFieldNames
        } else {
            require(primaryKeyFieldNames.all { fldName -> srcTableDef.sortedFieldNames.contains(fldName) }) {
                val missingFields: Set<String> =
                    srcTableDef.sortedFieldNames.toSet().minus(primaryKeyFieldNames.toSet())
                "The following primary key field(s) were specified: ${primaryKeyFieldNames.joinToString(", ")}.  " +
                    "However, the table does not include the following fields: ${
                        missingFields.joinToString(", ")
                    }.  " +
                    "The table includes the following fields: ${srcTableDef.sortedFieldNames.joinToString(", ")}"
            }
            primaryKeyFieldNames
        }

    val fieldsFinal: Set<Field> =
        if (includeFields == null) {
            srcTableDef.fields
        } else {
            require(
                includeFields.all { fldName -> srcTableDef.sortedFieldNames.contains(fldName) }
            ) {
                val missingFields: Set<String> = includeFields.minus(pkFieldsFinal.toSet())
                "The includeFields specified, ${includeFields.joinToString(", ")}, does not include the " +
                    "following primary-key fields: ${
                        missingFields.joinToString(", ")
                    }."
            }
            srcTableDef.fields.filter { fld -> fld.name in includeFields }.toSet()
        }

    val fieldNamesFinal = fieldsFinal.map { fld -> fld.name }.toSet()

    val (destTableDefFinal, _) = copyTable(
        srcCon = srcCon,
        destCon = destCon,
        srcDialect = srcDialect,
        destDialect = destDialect,
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        includeFields = fieldNamesFinal,
        primaryKeyFields = pkFieldsFinal,
    )

    val srcTableDefFinal: Table =
        srcTableDef.copy(
            fields = fieldsFinal,
            primaryKeyFieldNames = pkFieldsFinal,
        )

    val compareFieldNamesFinal: Set<String> =
        if (compareFields == null) {
            fieldNamesFinal.minus(srcTableDefFinal.primaryKeyFieldNames)
        } else {
            fieldNamesFinal.filter { fldName -> fldName in compareFields }.toSet()
        }

    val lkpTableFieldNames = pkFieldsFinal.union(compareFieldNamesFinal)

    val srcLkpTable = srcTableDefFinal.subset(fieldNames = lkpTableFieldNames)
    val srcKeysSQL: String = src.adapter.select(table = srcLkpTable)
    val srcLkpRows: Set<Row> =
        src.executor.fetchRows(sql = srcKeysSQL, fields = srcTableDefFinal.fields)

    val destLkpTable = destTableDefFinal.subset(fieldNames = lkpTableFieldNames)
    val destKeysSQL: String = dest.adapter.select(table = destLkpTable)
    val destLkpRows = dest.executor.fetchRows(sql = destKeysSQL, fields = srcTableDefFinal.fields)

    val rowDiff: RowDiff =
        compareRows(
            old = destLkpRows,
            new = srcLkpRows,
            primaryKeyFields = pkFieldsFinal.toSet(),
            includeFields = lkpTableFieldNames,
            compareFields = compareFieldNamesFinal,
        )

    if (rowDiff.added.keys.count() > 0) {
        val selectSQL: String =
            src.adapter.selectKeys(table = srcTableDefFinal, primaryKeyValues = rowDiff.added.keys)
        val addedRows: Set<Row> =
            src.executor.fetchRows(sql = selectSQL, fields = srcTableDefFinal.fields)
        val insertSQL: String = src.adapter.add(table = destTableDefFinal, rows = addedRows)
        src.executor.execute(sql = insertSQL)
    }

    if (rowDiff.deleted.keys.count() > 0) {
        val deleteSQL: String =
            src.adapter.delete(table = srcTableDefFinal, primaryKeyValues = rowDiff.deleted.keys)
        src.executor.execute(sql = deleteSQL)
    }

    if (rowDiff.updated.count() > 0) {
        val selectSQL: String =
            src.adapter.selectKeys(
                table = srcTableDefFinal,
                primaryKeyValues = rowDiff.updated.keys
            )
        val fullRows: Set<Row> = src.executor.fetchRows(sql = selectSQL, fields = fieldsFinal)
        val updateSQL: String = dest.adapter.update(table = destTableDefFinal, rows = fullRows)
        dest.executor.execute(sql = updateSQL)
    }

    return SyncResult.success(
        srcSchema = srcSchema,
        srcTable = srcTable,
        destSchema = destSchema,
        destTable = destTable,
        added = rowDiff.added.count(),
        deleted = rowDiff.deleted.count(),
        updated = rowDiff.updated.count(),
    )
}
