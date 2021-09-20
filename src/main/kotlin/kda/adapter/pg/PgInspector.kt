package kda.adapter.pg

import kda.domain.*

class PgInspector(private val sqlExecutor: SQLExecutor) : Inspector {
  override fun inspectTable(
    schema: String?,
    table: String,
    maxFloatDigits: Int,
    primaryKeyFieldNames: List<String>?,
  ): Table {
    val whereClause = if (schema == null) "c.table_name = '$table'"
    else "c.table_schema = '$schema' AND c.table_name = '$table'"

    val sql = """
      SELECT
          c.column_name
      ,   c.data_type
      ,   CASE
              WHEN c.data_type = 'integer' AND c.column_default LIKE 'nextval%'
              THEN TRUE
              ELSE FALSE
          END AS autoincrement_flag
      ,   CASE
              WHEN c.is_nullable = 'NO' THEN FALSE
              ELSE TRUE
          END AS nullable_flag
      ,   c.character_maximum_length AS max_len
      ,   c.numeric_precision AS precision
      ,   c.numeric_scale AS scale
      ,   CASE WHEN data_type = 'boolean' THEN TRUE ELSE FALSE END AS is_bool_flag
      ,   CASE WHEN data_type = 'date' THEN TRUE ELSE FALSE END AS is_date_flag
      ,   CASE WHEN data_type LIKE 'timestamp%' THEN TRUE ELSE FALSE END AS is_datetime_flag
      ,   CASE WHEN data_type = 'double precision' THEN TRUE ELSE FALSE END AS is_float_flag
      ,   CASE WHEN data_type = 'numeric' THEN TRUE ELSE FALSE END AS is_decimal_flag
      ,   CASE WHEN data_type IN ('bit', 'bigint', 'integer') THEN TRUE ELSE FALSE END AS is_int_flag
      ,   CASE WHEN data_type IN ('character', 'character varying', 'text') THEN TRUE ELSE FALSE END AS is_text_flag
      FROM information_schema.columns AS c
      WHERE $whereClause
      ORDER BY c.column_name
    """
    val rows = sqlExecutor.fetchRows(
      sql,
      setOf(
        Field(name = "column_name", dataType = StringType(40)),
        Field(name = "data_type", dataType = StringType(40)),
        Field(name = "autoincrement_flag", dataType = BoolType),
        Field(name = "nullable_flag", dataType = BoolType),
        Field(name = "max_len", dataType = NullableIntType(false)),
        Field(name = "precision", dataType = NullableIntType(false)),
        Field(name = "scale", dataType = NullableIntType(false)),
        Field(name = "is_bool_flag", dataType = BoolType),
        Field(name = "is_date_flag", dataType = BoolType),
        Field(name = "is_datetime_flag", dataType = BoolType),
        Field(name = "is_float_flag", dataType = BoolType),
        Field(name = "is_decimal_flag", dataType = BoolType),
        Field(name = "is_int_flag", dataType = BoolType),
        Field(name = "is_text_flag", dataType = BoolType),
      ),
    )
    val fields = rows.map { row ->
      val sqlDataType = row.value("data_type").value as String
      val fieldName = row.value("column_name").value as String
      val isAutoincrement = row.value("autoincrement_flag").value as Boolean
      val isNullable = row.value("nullable_flag").value as Boolean
      val maxLen = row.value("max_len").value as Int?
      val precision = row.value("precision").value as Int?
      val scale = row.value("scale").value as Int?
      val isBool = row.value("is_bool_flag").value as Boolean
      val isDate = row.value("is_date_flag").value as Boolean
      val isDateTime = row.value("is_datetime_flag").value as Boolean
      val isFloat = row.value("is_float_flag").value as Boolean
      val isDecimal = row.value("is_decimal_flag").value as Boolean
      val isInt = row.value("is_int_flag").value as Boolean
      val isString = row.value("is_text_flag").value as Boolean

      val dataType = when {
        isBool && isNullable -> NullableBoolType
        isBool -> BoolType
        isDate && isNullable -> NullableLocalDateType
        isDate -> LocalDateType
        isDateTime && isNullable -> NullableLocalDateTimeType
        isDateTime -> LocalDateTimeType
        isInt && isNullable -> IntType(autoincrement = isAutoincrement)
        isInt -> IntType(autoincrement = isAutoincrement)
        isFloat && isNullable -> NullableFloatType(maxDigits = maxFloatDigits)
        isFloat -> FloatType(4)
        isDecimal && isNullable -> NullableDecimalType(precision = precision ?: 19, scale = scale ?: 4)
        isDecimal -> DecimalType(precision = precision ?: 19, scale = scale ?: 4)
        isString && isNullable -> NullableStringType(maxLength = maxLen)
        isString -> StringType(maxLength = maxLen)
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
    val whereClause = if (schema == null) "t.table_name = '$table'"
    else "t.table_schema = '$schema' AND t.table_name = '$table'"

    val sql = """
      SELECT COUNT(*) AS ct
      FROM information_schema.tables AS t
      WHERE $whereClause
    """
    val ct = sqlExecutor.fetchInt(sql)
    return ct == 1
  }

  fun primaryKeyFields(schema: String?, table: String): List<String> {
    val fullTableName = if (schema == null) table else "$schema.$table"

    val sql = """
        SELECT
            pga.attname AS column_name
        FROM
            pg_index AS pgi
        ,   pg_class AS pgc
        ,   pg_attribute AS pga
        ,   pg_namespace AS pgn
        WHERE
            pgc.oid = '$fullTableName'::REGCLASS
            AND pgi.indrelid = pgc.oid
            AND pgc.relnamespace = pgn.oid
            AND pga.attrelid = pgc.oid
            AND pga.attnum = ANY(pgi.indkey)
            AND pgi.indisprimary
      """
    val rows = sqlExecutor.fetchRows(
      sql = sql,
      fields = setOf(Field("column_name", dataType = StringType(maxLength = null))),
    )
    return rows.map { row -> row.value("column_name").value as String }
  }
}
