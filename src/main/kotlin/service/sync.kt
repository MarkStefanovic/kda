package service
//
//import adapter.JdbcExecutor
//import adapter.pg.pgDatasource
//import domain.*
//import java.sql.Connection
//import java.sql.DriverManager
//
//fun sync(
//    srcURL: String,
//    destURL: String,
//    srcDialect: Dialect,
//    destDialect: Dialect,
//    srcSchema: String?,
//    srcTable: String,
//    destSchema: String?,
//    destTable: String,
//    pkFields: Set<String>? = null,
//    includeFields: Set<String>? = null,
//    maxFloatDigits: Int = 5,
//): SyncResult {
//    if (pkFields != null)
//        require(pkFields.count() > 0) {
//            "If pkFields is provided, then it must contain at least one field name."
//        }
//
//    if (includeFields != null)
//        require(includeFields.count() > 0) {
//            "If includeFields is provided, then it must contain at least one field name."
//        }
//
//
//    DriverManager.getConnection(srcURL).use { srcCon ->
//        val src = when (srcDialect) {
//            Dialect.HortonworksHive -> TODO()
//            Dialect.PostgreSQL      -> pgDatasource(srcCon)
//        }
//
//        var srcTableDef =
//            src.inspector.inspectTable(
//                schema = srcSchema,
//                table = srcTable,
//                maxFloatDigits = maxFloatDigits
//            )
//
//        if (pkFields != null) {
//            require(pkFields.all { fldName -> srcTableDef.sortedFieldNames.contains(fldName) }) {
//                val missingFields: Set<String> = srcTableDef.sortedFieldNames.toSet().minus(pkFields)
//                "The primary key field(s) were specified, ${pkFields.joinToString(", ")}.  " +
//                    "However, the table does not include the following fields: ${missingFields.joinToString(", ")}.  " +
//                    "Available fields include the following: ${srcTableDef.sortedFieldNames.joinToString(", ")}"
//            }
//            srcTableDef = srcTableDef.copy(primaryKeyFields = pkFields.toList())
//        }
//
//        val defaultResult =
//            SyncResult.success(
//                srcSchema = srcSchema,
//                srcTable = srcTable,
//                destSchema = destSchema,
//                destTable = destTable,
//                added = 0,
//                deleted = 0,
//                updated = 0,
//            )
//
//        if (includeFields != null) srcTableDef = srcTableDef.subset(fieldNames = includeFields)
//
//        val destTableDef = srcTableDef.copy(schema = destSchema, name = destTable)
//
//        if (!dest.inspector.tableExists(schema = destSchema, table = destTable)) {
//            val createTableSQL = dest.adapter.createTable(destTableDef)
//            dest.executor.execute(createTableSQL)
//        }
//
//        return result
//    }
//
//
//
//}
