package kda.adapter.hive

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Inspector
import kda.domain.NullableDecimalType
import kda.domain.NullableIntType
import kda.domain.NullableLocalDateTimeType
import kda.domain.NullableLocalDateType
import kda.domain.NullableStringType
import kda.domain.Table
import java.sql.Connection

class HiveInspector(private val con: Connection) : Inspector {
  override fun inspectTable(
    schema: String?,
    table: String,
    maxFloatDigits: Int,
    primaryKeyFieldNames: List<String>?,
  ): Table {
    require(primaryKeyFieldNames != null) {
      "The primary key column names must be provided for a Hive table, as they cannot be inspected."
    }
    val fullTableName = if (schema == null) {
      table
    } else {
      "$schema.$table"
    }
    val sql = "DESCRIBE $fullTableName"

    val fields: MutableList<Field> = mutableListOf()
    con.createStatement().use { stmt ->
      stmt.executeQuery(sql).use { rs ->
        while (rs.next()) {
          val colName = rs.getString(1)
          val dbDataType = rs.getString(2)
          val dataType: DataType<*> = if ("varchar" in dbDataType) {
            parseVarcharDbDataType(dbDataType)
          } else if ("decimal" in dbDataType) {
            parseDecimalDbDataType(dbDataType)
          } else {
            when (dbDataType) {
              "bigint" -> NullableIntType(false)
              "int" -> NullableIntType(false)
              "date" -> NullableLocalDateType
              "string" -> NullableStringType(null)
              "timestamp" -> NullableLocalDateTimeType
              else -> throw NotImplementedError("dbDataType '$dbDataType' is not recognized.")
            }
          }
          val field = Field(name = colName, dataType = dataType)
          fields.add(field)
        }
      }
    }
    return Table(
      schema = schema,
      name = table,
      fields = fields.toSet(),
      primaryKeyFieldNames = primaryKeyFieldNames,
    )
  }

  override fun tableExists(schema: String?, table: String): Boolean {
    val sql = if (schema == null) {
      "SHOW TABLES LIKE '$table'"
    } else {
      "SHOW TABLES IN $schema LIKE '$table'"
    }
    con.createStatement().use { stmt ->
      stmt.executeQuery(sql).use { rs ->
        return rs.next()
      }
    }
  }
}

fun parseDecimalDbDataType(dbDataType: String): NullableDecimalType {
  val pattern = "^decimal\\((\\d+),(\\d+)\\)$".toRegex()
  val match = pattern.find(dbDataType)
  return NullableDecimalType(precision = match!!.groupValues[1].toInt(), scale = match.groupValues[2].toInt())
}

fun parseVarcharDbDataType(dbDataType: String): NullableStringType {
  val pattern = "^varchar\\((\\d+)\\)$".toRegex()
  val match = pattern.find(dbDataType)
  return NullableStringType(match?.groupValues?.get(1)?.toInt())
}
