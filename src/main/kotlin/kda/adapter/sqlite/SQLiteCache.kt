@file:Suppress("SqlResolve")

package kda.adapter.sqlite

import kda.adapter.tableExists
import kda.domain.Cache
import kda.domain.DataType
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Table
import java.sql.Connection
import java.sql.Types

class SQLiteCache(
  val con: Connection,
  val showSQL: Boolean,
) : Cache {
  init {
    if (!tableExists(con = con, schema = null, table = "table_def")) {
      val tableDefSQL = """
        |CREATE TABLE table_def (
        |   schema_name TEXT NOT NULL
        |,  table_name TEXT NOT NULL CHECK (LENGTH(table_name) > 0)
        |,  field_name TEXT NOT NULL CHECK (LENGTH(field_name) > 0)
        |,  data_type TEXT NOT NULL CHECK (LENGTH(data_type) > 0)
        |,  nullable BOOLEAN NOT NULL CHECK (nullable IN (0, 1))
        |,  max_length INTEGER NULL CHECK (max_length IS NULL OR LENGTH(max_length) > 0)
        |,  precision INTEGER NULL CHECK (precision IS NULL OR LENGTH(precision) > 0)
        |,  scale INTEGER NULL CHECK (scale IS NULL OR LENGTH(scale) > 0)
        |,  date_added DATETIME NOT NULL DEFAULT current_timestamp
        |,  PRIMARY KEY (table_name, schema_name, field_name)
        |)
      """.trimMargin()

      if (showSQL) {
        println(
          """
          |SQLiteCache.init - create table_def table:
          |  ${tableDefSQL.split("\n").joinToString("\n  ")}
        """.trimMargin()
        )
      }

      con.createStatement().use { statement ->
        statement.execute(tableDefSQL)
      }
    }

    if (!tableExists(con = con, schema = null, table = "pk")) {
      val pkSQL = """
        |CREATE TABLE pk (
        |   schema_name TEXT NOT NULL
        |,  table_name TEXT NOT NULL CHECK (LENGTH(table_name) > 0)
        |,  field_name TEXT NOT NULL CHECK (LENGTH(field_name) > 0)
        |,  ix INT NOT NULL CHECK (ix >= 0)
        |,  date_added DATETIME NOT NULL DEFAULT current_timestamp
        |,  PRIMARY KEY (table_name, schema_name, field_name)
        |)
      """.trimMargin()

      if (showSQL) {
        println(
          """
          |SQLiteCache.init - create pk table:
          |  ${pkSQL.split("\n").joinToString("\n  ")}
        """.trimMargin()
        )
      }

      con.createStatement().use { statement ->
        statement.execute(pkSQL)
      }
    }
  }

  @Suppress("SqlInsertValues")
  override fun addTable(schema: String?, table: Table) {
    val insertFieldSQL = """
      |INSERT OR REPLACE INTO table_def (
      |   schema_name
      |,  table_name
      |,  field_name
      |,  data_type
      |,  nullable
      |,  max_length
      |,  precision
      |,  scale
      |) VALUES (
      |   ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |)
    """.trimMargin()

    if (showSQL) {
      val params = table.fields.mapIndexed { ix, field ->
        "[" + listOf(schema ?: "", table.name, field.name, ix) + "]"
      }.joinToString(", ")

      println(
        """
        |SQLiteCache.addTable insertFieldSQL - add field to table_def: 
        |$insertFieldSQL
        |PARAMS:
        |  $params
      """.trimMargin()
      )
    }

    con.prepareStatement(insertFieldSQL).use { preparedStatement ->
      table.fields.forEach { field ->
        preparedStatement.setString(1, schema ?: "")
        preparedStatement.setString(2, table.name)
        preparedStatement.setString(3, field.name)
        preparedStatement.setString(4, field.dataType.name)
        preparedStatement.setBoolean(5, field.dataType.nullable)
        val maxLength: Int? = when (field.dataType) {
          is DataType.text -> field.dataType.maxLength
          is DataType.nullableText -> field.dataType.maxLength
          else -> null
        }
        if (maxLength == null) {
          preparedStatement.setNull(6, Types.INTEGER)
        } else {
          preparedStatement.setInt(6, maxLength)
        }
        when (field.dataType) {
          is DataType.decimal -> {
            preparedStatement.setInt(7, field.dataType.precision)
            preparedStatement.setInt(8, field.dataType.scale)
          }
          is DataType.nullableDecimal -> {
            preparedStatement.setInt(7, field.dataType.precision)
            preparedStatement.setInt(8, field.dataType.scale)
          }
          else -> {
            preparedStatement.setNull(7, Types.INTEGER)
            preparedStatement.setNull(8, Types.INTEGER)
          }
        }
        preparedStatement.addBatch()
      }
      preparedStatement.executeBatch()
    }

    val insertPKsql = """
      |INSERT OR REPLACE INTO pk (
      |   schema_name
      |,  table_name
      |,  field_name
      |,  ix
      |) VALUES (
      |   ?
      |,  ?
      |,  ?
      |,  ?
      |)
    """.trimMargin()

    if (showSQL) {
      val params = table.primaryKeyFieldNames.mapIndexed { ix, fieldName ->
        "[" + listOf(schema ?: "", table.name, fieldName, ix) + "]"
      }.joinToString(", ")

      println(
        """
        |SQLiteCache.addTable - add primary key field to pk table:
        |  SQL:
        |    ${insertPKsql.split("\n").joinToString("\n    ")}
        |  Parameters:  
        |    $params
      """.trimMargin()
      )
    }

    con.prepareStatement(insertPKsql).use { preparedStatement ->
      table.primaryKeyFieldNames.forEachIndexed { ix, fieldName ->
        preparedStatement.setString(1, schema ?: "")
        preparedStatement.setString(2, table.name)
        preparedStatement.setString(3, fieldName)
        preparedStatement.setInt(4, ix)

        preparedStatement.addBatch()
      }
      preparedStatement.executeBatch()
    }
  }

  override fun getTable(schema: String?, table: String): Table? {
    val fetchFieldsSQL = """
      |SELECT 
      |   t.field_name
      |,  t.data_type
      |,  t.nullable
      |,  t.max_length
      |,  t.precision
      |,  t.scale
      |FROM table_def AS t
      |WHERE
      |   t.schema_name = ?
      |   AND t.table_name = ?
      |ORDER BY 
      |   t.field_name
    """.trimMargin()

    if (showSQL) {
      println(
        """
        |SQLiteCache.getTable - get fields from table_def:
        |$fetchFieldsSQL
        |PARAMS:
        |  schema_name: $schema
        |  table_name: $table
      """.trimMargin()
      )
    }

    val fields = mutableListOf<Field<*>>()
    con.prepareStatement(fetchFieldsSQL).use { preparedStatement ->
      preparedStatement.setString(1, schema ?: "")
      preparedStatement.setString(2, table)

      preparedStatement.executeQuery().use { rs ->
        while (rs.next()) {
          val fieldName = rs.getString("field_name")
          val dataTypeName = rs.getString("data_type")
          val maxLength = rs.getObject("max_length") as Int?
          val precision = rs.getObject("precision") as Int? ?: 18
          val scale = rs.getObject("scale") as Int? ?: 4

          val dataType = when (dataTypeName) {
            "bool" -> DataType.bool
            "nullableBool" -> DataType.nullableBool
            "bigInt" -> DataType.bigInt
            "nullableBigInt" -> DataType.nullableBigInt
            "decimal" -> DataType.decimal(precision = precision, scale = scale)
            "nullableDecimal" -> DataType.nullableDecimal(precision = precision, scale = scale)
            "float" -> DataType.float
            "nullableFloat" -> DataType.nullableFloat
            "int" -> DataType.int
            "nullableInt" -> DataType.nullableInt
            "localDate" -> DataType.localDate
            "nullableLocalDate" -> DataType.nullableLocalDate
            "localDateTime" -> DataType.localDateTime
            "nullableLocalDateTime" -> DataType.nullableLocalDateTime
            "text" -> DataType.text(maxLength = maxLength)
            "nullableText" -> DataType.nullableText(maxLength = maxLength)
            else -> throw KDAError.UnrecognizeDataType(dataTypeName)
          }

          val field = Field(name = fieldName, dataType = dataType)

          fields.add(field)
        }
      }
    }

    return if (fields.isEmpty()) {
      null
    } else {
      val fetchPKsql = """
        |SELECT 
        |   pk.field_name
        |FROM pk
        |WHERE
        |   pk.schema_name = ?
        |   AND pk.table_name = ?
        |ORDER BY 
        |   pk.ix
      """.trimMargin()

      if (showSQL) {
        println(
          """
          |SQLiteCache.getTable - get primary key fields:
          |$fetchPKsql
          |PARAMS:
          |  schema_name: $schema
          |  table_name: $table
        """.trimMargin()
        )
      }

      val primaryKeyFieldNames = mutableListOf<String>()
      con.prepareStatement(fetchPKsql).use { preparedStatement ->
        preparedStatement.setString(1, schema ?: "")
        preparedStatement.setString(2, table)

        preparedStatement.executeQuery().use { rs ->
          while (rs.next()) {
            val pkFieldName = rs.getString("field_name")
            primaryKeyFieldNames.add(pkFieldName)
          }
        }
      }

      Table(
        name = table,
        fields = fields.toSet(),
        primaryKeyFieldNames = primaryKeyFieldNames,
      )
    }
  }
}
