package kda.adapter.mssql

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Inspector
import kda.domain.SQLExecutor
import kda.domain.Table

class MSSQLInspector(private val sqlExecutor: SQLExecutor) : Inspector {
  override fun inspectTable(
    schema: String?,
    table: String,
    maxFloatDigits: Int,
    primaryKeyFieldNames: List<String>?,
  ): Table {
    val whereClause = if (schema == null) "c.[table_name] = '$table'"
    else "c.[table_schema] = '$schema' AND c.[table_name] = '$table'"

    val sql = """
      SELECT
          LOWER(c.[column_name]) AS column_name
      ,   c.[data_type]
      ,   CASE
              WHEN c.[data_type] = 'integer' AND c.[column_default] LIKE 'nextval%'
              THEN 1
              ELSE 0
          END AS autoincrement_flag
      ,   CASE
              WHEN c.[is_nullable] = 'NO' THEN 0
              ELSE 1
          END AS nullable_flag
      ,   c.[character_maximum_length] AS max_len
      ,   c.[numeric_precision] AS precision
      ,   c.[numeric_scale] AS scale
      ,   0 AS is_bool_flag
      ,   CASE WHEN [data_type] = 'date' THEN 1 ELSE 0 END AS is_date_flag
      ,   CASE WHEN [data_type] = 'datetime' THEN 1 ELSE 0 END AS is_datetime_flag
      ,   CASE WHEN [data_type] IN ('double', 'float') THEN 1 ELSE 0 END AS is_float_flag
      ,   CASE WHEN [data_type] IN ('money', 'decimal', 'numeric') THEN 1 ELSE 0 END AS is_decimal_flag
      ,   CASE WHEN [data_type] IN ('bit', 'bigint', 'int', 'smallint') THEN 1 ELSE 0 END AS is_int_flag
      ,   CASE WHEN [data_type] IN ('char', 'nchar', 'ntext', 'nvarchar', 'text', 'uniqueidentifier', 'varchar') THEN 1 ELSE 0 END AS is_text_flag
      FROM [information_schema].[columns] AS c
      WHERE $whereClause
      ORDER BY c.[column_name]
    """
    val rows = sqlExecutor.fetchRows(
      sql,
      setOf(
        Field(name = "column_name", dataType = DataType.text(40)),
        Field(name = "data_type", dataType = DataType.text(40)),
        Field(name = "autoincrement_flag", dataType = DataType.bool),
        Field(name = "nullable_flag", dataType = DataType.bool),
        Field(name = "max_len", dataType = DataType.nullableInt(false)),
        Field(name = "precision", dataType = DataType.nullableInt(false)),
        Field(name = "scale", dataType = DataType.nullableInt(false)),
        Field(name = "is_bool_flag", dataType = DataType.bool),
        Field(name = "is_date_flag", dataType = DataType.bool),
        Field(name = "is_datetime_flag", dataType = DataType.bool),
        Field(name = "is_float_flag", dataType = DataType.bool),
        Field(name = "is_decimal_flag", dataType = DataType.bool),
        Field(name = "is_int_flag", dataType = DataType.bool),
        Field(name = "is_text_flag", dataType = DataType.bool),
      ),
    )
    val fields = rows.map { row ->
      val sqlDataType = row.value("data_type").value as String
      val fieldName = row.value("column_name").value as String
      val isAutoincrement = row.value("autoincrement_flag").value as Boolean
      val isNullable = row.value("nullable_flag").value as Boolean
      val maxLen = row.value("max_len").value as Int?
      val precision = if (sqlDataType == "money") {
        19
      } else {
        row.value("precision").value as Int?
      }
      val scale = if (sqlDataType == "money") {
        4
      } else {
        row.value("scale").value as Int?
      }
      val isBool = row.value("is_bool_flag").value as Boolean
      val isDate = row.value("is_date_flag").value as Boolean
      val isDateTime = row.value("is_datetime_flag").value as Boolean
      val isFloat = row.value("is_float_flag").value as Boolean
      val isDecimal = row.value("is_decimal_flag").value as Boolean
      val isInt = row.value("is_int_flag").value as Boolean
      val isString = row.value("is_text_flag").value as Boolean

      val dataType = when {
        isBool && isNullable -> DataType.nullableBool
        isBool -> DataType.bool
        isDate && isNullable -> DataType.nullableLocalDate
        isDate -> DataType.localDate
        isDateTime && isNullable -> DataType.nullableLocalDateTime
        isDateTime -> DataType.localDateTime
        isInt && isNullable -> DataType.nullableInt(autoincrement = isAutoincrement)
        isInt -> DataType.int(autoincrement = isAutoincrement)
        isFloat && isNullable -> DataType.nullableFloat(maxDigits = maxFloatDigits)
        isFloat -> DataType.float(4)
        isDecimal && isNullable -> DataType.nullableDecimal(precision = precision ?: 19, scale = scale ?: 4)
        isDecimal -> DataType.decimal(precision = precision ?: 19, scale = scale ?: 4)
        isString && isNullable -> DataType.nullableText(maxLength = maxLen)
        isString -> DataType.text(maxLength = maxLen)
        else -> throw NotImplementedError("Could not recognize the data type, '$sqlDataType'.")
      }
      Field(name = fieldName, dataType = dataType)
    }
    val pkFields = primaryKeyFieldNames ?: primaryKeyFields(schema = schema, table = table)
    return Table(
      schema = schema,
      name = table,
      fields = fields.toSet(),
      primaryKeyFieldNames = pkFields,
    )
  }

  override fun tableExists(schema: String?, table: String): Boolean {
    val whereClause = if (schema == null) "t.[table_name] = '$table'"
    else "t.[table_schema] = '$schema' AND t.[table_name] = '$table'"

    val sql = """
      SELECT COUNT(*) AS ct
      FROM [information_schema].[tables] AS t
      WHERE $whereClause
    """
    val ct = sqlExecutor.fetchInt(sql)
    return ct == 1
  }

  fun primaryKeyFields(schema: String?, table: String): List<String> {
    val finalSchema = schema ?: "dbo"
    val sql = """
      SELECT
          kcu.[column_name]
      FROM [information_schema].[key_column_usage] AS kcu
      WHERE
          OBJECTPROPERTY(
              OBJECT_ID(kcu.[constraint_schema] + '.' + QUOTENAME(kcu.[constraint_name]))
          ,   'IsPrimaryKey'
          ) = 1
          AND kcu.[table_schema] = '$finalSchema'
          AND kcu.[table_name] = '$table'
      ORDER BY
          kcu.[ordinal_position]
      """
    val rows = sqlExecutor.fetchRows(
      sql = sql,
      fields = setOf(Field("column_name", dataType = DataType.text(maxLength = null))),
    )
    return rows.map { row -> row.value("column_name").value as String }.toList()
  }
}
