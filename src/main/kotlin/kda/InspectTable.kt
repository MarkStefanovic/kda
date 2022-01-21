package kda

import kda.adapter.pg.PgCache
import kda.adapter.sqlite.SQLiteCache
import kda.domain.DbDialect
import kda.domain.KDAError
import kda.domain.Table
import java.sql.Connection

fun inspectTable(
  con: Connection,
  cacheCon: Connection,
  cacheDialect: DbDialect,
  cacheSchema: String?,
  schema: String?,
  table: String,
  primaryKeyFieldNames: List<String>,
  includeFieldNames: Set<String>?,
  showSQL: Boolean = false,
): Table {
  if ((includeFieldNames != null) && (includeFieldNames.isEmpty())) {
    throw KDAError.InvalidArgument(
      errorMessage = "If includeFieldNames is not null, then it must have at least 1 field.",
      argumentName = "includeFieldNames",
      argumentValue = includeFieldNames,
    )
  } else if (primaryKeyFieldNames.isEmpty()) {
    throw KDAError.InvalidArgument(
      errorMessage = "primaryKeyFieldNames cannot be blank.",
      argumentName = "primaryKeyFieldNames",
      argumentValue = primaryKeyFieldNames,
    )
  } else {
    val cache = when (cacheDialect) {
      DbDialect.HH -> TODO()
      DbDialect.MSSQL -> TODO()
      DbDialect.PostgreSQL -> PgCache(
        con = cacheCon,
        cacheSchema = cacheSchema ?: error("cacheSchema is required"),
        showSQL = showSQL,
      )
      DbDialect.SQLite -> SQLiteCache(
        con = cacheCon,
        showSQL = showSQL,
      )
    }

    val cachedTable = cache.getTable(schema = schema, table = table)

    val tableDef =
      if (cachedTable == null) {
        val def = kda.adapter.inspectTable(
          con = con,
          schema = schema,
          table = table,
          hardCodedPrimaryKeyFieldNames = primaryKeyFieldNames,
        )
        cache.addTable(schema = schema, table = def)
        def
      } else {
        cachedTable
      }

    val missingPrimaryKeyFields: Set<String> =
      primaryKeyFieldNames
        .toSet()
        .minus(tableDef.fields.map { it.name }.toSet())

    return if (missingPrimaryKeyFields.isEmpty()) {
      val missingIncludeFields: Set<String> =
        includeFieldNames
          ?.minus(tableDef.fields.map { it.name }.toSet())
          ?: setOf()

      if (missingIncludeFields.isNotEmpty()) {
        val includeFieldsCSV = includeFieldNames?.joinToString(", ") ?: ""

        val missingIncludeFieldsCSV = missingPrimaryKeyFields.joinToString(", ")

        val errorMessage =
          "The includeFields specified, [$includeFieldsCSV], does not include the " +
            "following primary-key fields: $missingIncludeFieldsCSV."

        throw KDAError.InvalidArgument(
          errorMessage = errorMessage,
          argumentName = "includeFieldNames",
          argumentValue = "[$includeFieldsCSV]",
        )
      } else {
        if (includeFieldNames == null) {
          tableDef
        } else {
          val finalFields =
            tableDef
              .fields
              .filter { fld -> fld.name in includeFieldNames }
              .toSet()

          tableDef.copy(fields = finalFields)
        }
      }
    } else {
      val pkFieldNamesCSV = primaryKeyFieldNames.joinToString(", ")

      val missingFieldsCSV = missingPrimaryKeyFields.joinToString(", ")

      val fieldNameCSV = tableDef.fields.map { it.name }.sorted().joinToString(", ")

      val errorMessage =
        "The following primary key field(s) were specified: $pkFieldNamesCSV.  " +
          "However, the table does not include the following fields: $missingFieldsCSV.  " +
          "The table includes the following fields: $fieldNameCSV"

      throw KDAError.InvalidArgument(
        errorMessage = errorMessage,
        argumentName = "primaryKeyFieldNames",
        argumentValue = primaryKeyFieldNames,
      )
    }
  }
}
