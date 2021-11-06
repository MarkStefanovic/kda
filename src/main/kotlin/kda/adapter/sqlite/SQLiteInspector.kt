package kda.adapter.sqlite

import kda.domain.*

class SQLiteInspector(private val sqlExecutor: SQLExecutor) : Inspector {
  override fun inspectTable(
    schema: String?,
    table: String,
    maxFloatDigits: Int,
    primaryKeyFieldNames: List<String>?,
  ): Table {
    val pragmaRows =
      sqlExecutor
        .fetchRows(
          "PRAGMA table_info('$table')",
          setOf(
            Field(name = "cid", dataType = DataType.int(true)),
            Field(name = "name", dataType = DataType.text(40)),
            Field(name = "type", dataType = DataType.text(null)),
            Field(name = "notnull", dataType = DataType.int(false)),
            Field(name = "pk", dataType = DataType.int(false)),
          ),
        )
        .map { row ->
          PragmaRow(
            columnName = row.value("name").value as String,
            type = row.value("type").value as String,
            notNull = row.value("notnull").value as Int,
            pkOrder = row.value("pk").value as Int,
          )
        }
        .toSet()

    val fields =
      pragmaRows.map { row ->
        val fixedCharLengthTypes = setOf(
          "CHAR",
          "CHARACTER",
          "VARCHAR",
          "VARYING_CHARACTER",
          "NCHAR(55)",
          "NATIVE CHARACTER",
          "NVARCHAR",
        )
        val maxLen = if (fixedCharLengthTypes.any { row.type.startsWith(it) }) {
          Regex("(\\d+)").find(row.type)?.groupValues?.first()?.toInt()
        } else {
          null
        }

        val (precision, scale) = if (row.type.startsWith("NUMERIC") || row.type.startsWith("DECIMAL")) {
          val groups = Regex("(\\d+),(\\d+)").find(row.type)?.groupValues
          if (groups == null) {
            null to null
          } else {
            groups[1].toInt() to groups[2].toInt()
          }
        } else {
          null to null
        }

        val isNullable = row.notNull == 0
        val isBool = row.type == "BOOLEAN"
        val isDate = row.type == "DATE"
        val isDateTime = row.type == "DATETIME"
        val isFloat = row.type in setOf("FLOAT", "REAL") || ((row.type.startsWith("NUMERIC") || row.type.startsWith("DECIMAL")) && "," !in row.type)
        val isDecimal = "," in row.type && (row.type.startsWith("NUMERIC") || row.type.startsWith("DECIMAL"))
        val isInt = row.type in setOf("INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "UNSIGNED BIG INT", "INT2", "INT8")
        val isString = (fixedCharLengthTypes + "TEXT").any { row.type.startsWith(it) }

        val dataType =
          when {
            isBool && isNullable -> DataType.nullableBool
            isBool -> DataType.bool
            isDate && isNullable -> DataType.nullableLocalDate
            isDate -> DataType.localDate
            isDateTime && isNullable -> DataType.nullableLocalDateTime
            isDateTime -> DataType.localDateTime
            isInt && isNullable -> DataType.int(autoincrement = false)
            isInt -> DataType.int(autoincrement = false)
            isFloat && isNullable -> DataType.nullableFloat(maxDigits = maxFloatDigits)
            isFloat -> DataType.float(4)
            isDecimal && isNullable ->
              DataType.nullableDecimal(precision = precision ?: 19, scale = scale ?: 4)
            isDecimal -> DataType.decimal(precision = precision ?: 19, scale = scale ?: 4)
            isString && isNullable -> DataType.nullableText(maxLength = maxLen)
            isString -> DataType.text(maxLength = maxLen)
            else ->
              throw NotImplementedError("Could not recognize the data type, '${row.type}'.")
          }

        Field(name = row.columnName, dataType = dataType)
      }

    val dbPKFields =
      pragmaRows
        .filter { it.pkOrder > 0 }
        .sortedBy { it.pkOrder }
        .map { it.columnName }

    val finalPKFields = dbPKFields.ifEmpty {
      primaryKeyFieldNames
    }

    if (finalPKFields.isNullOrEmpty()) {
      throw KDAError.NoPrimaryKeySpecified(table = table)
    }

    return Table(
      schema = null,
      name = table,
      fields = fields.toSet(),
      primaryKeyFieldNames = finalPKFields,
    )
  }

  override fun tableExists(schema: String?, table: String): Boolean {
    val sql =
      """
      SELECT 
        COUNT(*) AS ct
      FROM sqlite_master AS t
      WHERE 
        t.type = 'table'
        AND t.name = '$table'
    """
    val ct = sqlExecutor.fetchInt(sql)
    return ct == 1
  }
}

private data class PragmaRow(
  val columnName: String,
  val type: String,
  val notNull: Int,
  val pkOrder: Int,
)
