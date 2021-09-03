package kda.adapter.hive

import kda.domain.Field
import kda.domain.Inspector
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
          val dataType: kda.domain.DataType<*> = if ("varchar" in dbDataType) {
            parseVarcharDbDataType(dbDataType)
          } else if ("decimal" in dbDataType) {
            parseDecimalDbDataType(dbDataType)
          } else {
            when (dbDataType) {
              "bigint" -> kda.domain.IntType(false)
              "int" -> kda.domain.IntType(false)
              "date" -> kda.domain.LocalDateType
              "string" -> kda.domain.StringType(null)
              "timestamp" -> kda.domain.LocalDateTimeType
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

fun parseDecimalDbDataType(dbDataType: String): kda.domain.DecimalType {
  val pattern = "^decimal\\((\\d+),(\\d+)\\)$".toRegex()
  val match = pattern.find(dbDataType)
  return kda.domain.DecimalType(precision = match!!.groupValues[1].toInt(), scale = match.groupValues[2].toInt())
}

fun parseVarcharDbDataType(dbDataType: String): kda.domain.StringType {
  val pattern = "^varchar\\((\\d+)\\)$".toRegex()
  val match = pattern.find(dbDataType)
  return kda.domain.StringType(match?.groupValues?.get(1)?.toInt())
}
