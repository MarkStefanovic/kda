package kda.adapter

import kda.domain.DataType
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Table
import java.sql.Connection
import java.sql.Types

fun getPrimaryKeyFields(con: Connection, schema: String?, table: String): List<String>? {
  val fieldNames = mutableListOf<Pair<Int, String>>()
  con.metaData.getPrimaryKeys(null, schema, table).use { rs ->
    while (rs.next()) {
      val fieldName = rs.getString("COLUMN_NAME")

      val index = rs.getInt("KEY_SEQ")

      fieldNames.add(index to fieldName)
    }
  }

  return if (fieldNames.isEmpty()) {
    null
  } else {
    return fieldNames.sortedBy { it.first }.map { it.second }
  }
}

fun tableExists(con: Connection, schema: String?, table: String): Boolean =
  con.metaData.getTables(null, schema, table, arrayOf("TABLE")).use { rs ->
    rs.next()
  }

fun inspectTable(
  con: Connection,
  schema: String?,
  table: String,
  hardCodedPrimaryKeyFieldNames: List<String>? = null,
): Table? {
  val fields = mutableListOf<Field<*>>()
  con.metaData.getColumns(null, schema, table, null).use { rs ->
    while (rs.next()) {
      val dataType = rs.getInt("DATA_TYPE")
      val columnName = rs.getString("COLUMN_NAME")
      val nullable = rs.getString("IS_NULLABLE") == "YES"
      val columnSize = rs.getInt("COLUMN_SIZE") // applicable to text length and precision of a numeric
      val scale = rs.getInt("DECIMAL_DIGITS")
      val typeName = rs.getString("TYPE_NAME")
      val maxLength = if (columnSize > 4000) {
        null
      } else {
        columnSize
      }

      val field = when (dataType) {
        Types.BOOLEAN -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableBool)
        } else {
          Field(name = columnName, dataType = DataType.bool)
        }
        Types.BIGINT -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableBigInt)
        } else {
          Field(name = columnName, dataType = DataType.bigInt)
        }
        Types.BIT, Types.TINYINT, Types.SMALLINT, Types.INTEGER -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableInt)
        } else {
          Field(name = columnName, dataType = DataType.int)
        }
        Types.FLOAT, Types.REAL, Types.DOUBLE -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableFloat)
        } else {
          Field(name = columnName, dataType = DataType.float)
        }
        Types.NUMERIC, Types.DECIMAL -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableDecimal(precision = columnSize, scale = scale))
        } else {
          Field(name = columnName, dataType = DataType.decimal(precision = columnSize, scale = scale))
        }
        Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableText(maxLength = maxLength))
        } else {
          Field(name = columnName, dataType = DataType.text(maxLength = maxLength))
        }
        Types.DATE -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableLocalDate)
        } else {
          Field(name = columnName, dataType = DataType.localDate)
        }
        Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableLocalDateTime)
        } else {
          Field(name = columnName, dataType = DataType.localDateTime)
        }
        else -> throw KDAError.UnrecognizeDataType("$typeName: $dataType")
      }
      fields.add(field)
    }
  }

  val primaryKeyFieldNames: List<String> =
    hardCodedPrimaryKeyFieldNames
      ?: getPrimaryKeyFields(con = con, schema = schema, table = table)
      ?: throw KDAError.TableMissingAPrimaryKey(schema = schema, table = table)

  return Table(
    name = table,
    fields = fields.toSet(),
    primaryKeyFieldNames = primaryKeyFieldNames,
  )
}
