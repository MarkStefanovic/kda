package kda.adapter.hive

import kda.domain.DataType
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
      "`$table`"
    } else {
      "`$schema`.`$table`"
    }
    val sql = "DESCRIBE $fullTableName"

    val fields: MutableList<Field> = mutableListOf()
    con.createStatement().use { stmt ->
      stmt.executeQuery(sql).use { rs ->
        while (rs.next()) {
          val colName = rs.getString(1)
          val dbDataType = rs.getString(2)
          val dataType: DataType<*> =
            if ("varchar" in dbDataType) {
              parseVarcharDbDataType(dbDataType)
            } else if ("char" in dbDataType) {
              parseCharDbDataType(dbDataType)
            } else if ("decimal" in dbDataType) {
              parseDecimalDbDataType(dbDataType)
            } else {
              when (dbDataType) {
                "bigint" -> DataType.nullableInt(false)
                "int" -> DataType.nullableInt(false)
                "date" -> DataType.nullableLocalDate
                "string" -> DataType.nullableText(null)
                "timestamp" -> DataType.nullableLocalDateTime
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
      "SHOW TABLES IN `$schema` LIKE '$table'"
    }
    con.createStatement().use { stmt ->
      stmt.executeQuery(sql).use { rs ->
        return rs.next()
      }
    }
  }
}

private fun parseDecimalDbDataType(dbDataType: String): DataType.nullableDecimal {
  val pattern = "^decimal\\((\\d+),(\\d+)\\)$".toRegex()
  val match = pattern.find(dbDataType)
  return DataType.nullableDecimal(precision = match!!.groupValues[1].toInt(), scale = match.groupValues[2].toInt())
}

private fun parseVarcharDbDataType(dbDataType: String): DataType.nullableText {
  val pattern = "^varchar\\((\\d+)\\)$".toRegex()
  val match = pattern.find(dbDataType)
  return DataType.nullableText(match?.groupValues?.get(1)?.toInt())
}

private fun parseCharDbDataType(dbDataType: String): DataType.nullableText {
  val pattern = "^char\\((\\d+)\\)$".toRegex()
  val match = pattern.find(dbDataType)
  return DataType.nullableText(match?.groupValues?.get(1)?.toInt())
}
