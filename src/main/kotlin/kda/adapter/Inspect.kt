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
  con.metaData.getTables(null, schema, table, arrayOf("TABLE", "VIEW")).use { rs ->
    rs.next()
  }

fun inspectTable(
  con: Connection,
  schema: String?,
  table: String,
  hardCodedPrimaryKeyFieldNames: List<String>? = null,
): Table {
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
        Types.BIT, Types.BOOLEAN -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableBool)
        } else {
          Field(name = columnName, dataType = DataType.bool)
        }
        Types.BIGINT -> if (nullable) {
          Field(name = columnName, dataType = DataType.nullableBigInt)
        } else {
          Field(name = columnName, dataType = DataType.bigInt)
        }
        Types.TINYINT, Types.SMALLINT, Types.INTEGER -> if (nullable) {
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
        Types.TIMESTAMP -> if (nullable) {
          if (typeName == "timestamptz") {
            Field(name = columnName, dataType = DataType.nullableTimestampUTC(precision = scale))
          } else {
            Field(name = columnName, dataType = DataType.nullableTimestamp(precision = scale))
          }
        } else {
          if (typeName == "timestamptz") {
            Field(name = columnName, dataType = DataType.timestampUTC(precision = scale))
          } else {
            Field(name = columnName, dataType = DataType.timestamp(precision = scale))
          }
        }
//        Types.TIMESTAMP_WITH_TIMEZONE -> if (nullable) {
//          Field(name = columnName, dataType = DataType.nullableTimestampUTC(precision = scale))
//        } else {
//          Field(name = columnName, dataType = DataType.timestampUTC(precision = scale))
//        }
        else -> throw KDAError.UnrecognizeDataType("$typeName: $dataType")
      }
      fields.add(field)
    }
  }

  val primaryKeyFieldNames: List<String> =
    hardCodedPrimaryKeyFieldNames
      ?: getPrimaryKeyFields(con = con, schema = schema, table = table)
      ?: throw KDAError.TableMissingAPrimaryKey(schema = schema, table = table)

  // primary-key fields cannot be nullable
  val finalFields = if (fields.any { field -> field.name in primaryKeyFieldNames && field.dataType.nullable }) {
    fields.map { field ->
      if (field.name in primaryKeyFieldNames) {
        Field(
          name = field.name,
          dataType = when (field.dataType) {
            DataType.nullableBigInt -> DataType.bigInt
            DataType.nullableBool -> DataType.bool
            is DataType.nullableDecimal -> DataType.decimal(precision = field.dataType.precision, scale = field.dataType.scale)
            DataType.nullableFloat -> DataType.float
            DataType.nullableInt -> DataType.int
            DataType.nullableLocalDate -> DataType.localDate
            is DataType.nullableTimestamp -> DataType.timestamp(precision = field.dataType.precision)
            is DataType.nullableText -> DataType.text(maxLength = field.dataType.maxLength)
            is DataType.nullableTimestampUTC -> DataType.nullableTimestampUTC(precision = field.dataType.precision)
            else -> field.dataType
          }
        )
      } else {
        field
      }
    }
  } else {
    fields
  }

  return Table(
    name = table,
    fields = finalFields.toSet(),
    primaryKeyFieldNames = primaryKeyFieldNames,
  )
}
