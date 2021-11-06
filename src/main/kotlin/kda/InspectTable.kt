package kda

import kda.domain.Dialect
import kda.domain.KDAError
import kda.domain.Table
import java.sql.Connection

fun inspectTable(
  con: Connection,
  dialect: Dialect,
  schema: String?,
  table: String,
  primaryKeyFieldNames: List<String>,
  includeFieldNames: Set<String>?,
  cache: Cache = sqliteCache,
): Result<Table?> = runCatching {
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
    val ds = datasource(con = con, dialect = dialect)

    val cachedTableDef = cache.tableDef(schema = schema ?: "", table = table).getOrThrow()

    val tableDef =
      if (cachedTableDef == null) {
        val def = ds.inspector.inspectTable(
          schema = schema,
          table = table,
          maxFloatDigits = 5,
          primaryKeyFieldNames = primaryKeyFieldNames,
        )
        cache.addTableDef(def).getOrThrow()
        def
      } else {
        cachedTableDef
      }

    val missingPrimaryKeyFields: Set<String> =
      primaryKeyFieldNames
        .toSet()
        .minus(tableDef.sortedFieldNames.toSet())

    if (missingPrimaryKeyFields.isEmpty()) {
      val missingIncludeFields: Set<String> =
        includeFieldNames
          ?.minus(tableDef.sortedFieldNames.toSet())
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

      val fieldNameCSV = tableDef.sortedFieldNames.joinToString(", ")

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
