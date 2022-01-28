@file:Suppress("DuplicatedCode")

package kda.adapter.pg

import kda.adapter.tableExists
import kda.domain.Cache
import kda.domain.DataType
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Table
import java.sql.Connection
import java.sql.Types

class PgCache(
  val con: Connection,
  val cacheSchema: String,
  val showSQL: Boolean,
) : Cache {
  val cache = mutableMapOf<Pair<String, String>, Table>()

  init {
    if (!tableExists(con = con, schema = cacheSchema, table = "table_def")) {
      //language=PostgreSQL
      val sql = """
        |CREATE TABLE IF NOT EXISTS $cacheSchema.table_def (
        |   schema_name TEXT NOT NULL
        |,  table_name TEXT NOT NULL CHECK (LENGTH(table_name) > 0)
        |,  field_name TEXT NOT NULL CHECK (LENGTH(field_name) > 0)
        |,  data_type TEXT NOT NULL CHECK (LENGTH(data_type) > 0)
        |,  nullable BOOLEAN NOT NULL
        |,  max_length INTEGER NULL CHECK (max_length IS NULL OR max_length > 0)
        |,  precision INTEGER NULL CHECK (precision IS NULL OR precision > 0)
        |,  scale INTEGER NULL CHECK (scale IS NULL OR scale >= 0)
        |,  pk_cols TEXT[] NOT NULL
        |,  date_added TIMESTAMPTZ NOT NULL DEFAULT now()
        |,  date_updated TIMESTAMPTZ NULL
        |,  PRIMARY KEY (table_name, schema_name, field_name)
        |)
      """.trimMargin()

      if (showSQL) {
        println(
          """
          |${javaClass.simpleName}.init - create table_def table:
          |  ${sql.split("\n").joinToString("\n  ")}
        """.trimMargin()
        )
      }

      con.createStatement().use { statement ->
        statement.execute(sql)
      }
    }
  }

  @Suppress("SqlInsertValues")
  override fun addTable(schema: String?, table: Table) {
    require(schema != null) { "Schema is required." }

    cache[schema to table.name] = table

    //language=PostgreSQL
    val sql = """
      |INSERT INTO $cacheSchema.table_def (
      |   schema_name
      |,  table_name
      |,  field_name
      |,  data_type
      |,  nullable
      |,  max_length
      |,  precision
      |,  scale
      |,  pk_cols
      |) VALUES (
      |   ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |,  ?
      |)
      |ON CONFLICT (
      |   table_name
      |,  schema_name
      |,  field_name
      |)
      |DO UPDATE SET
      |   data_type = EXCLUDED.data_type
      |,  nullable = EXCLUDED.nullable
      |,  max_length = EXCLUDED.max_length
      |,  precision = EXCLUDED.precision
      |,  scale = EXCLUDED.scale
      |,  pk_cols = EXCLUDED.pk_cols
      |,  date_updated = now()
      |WHERE 
      |(
      |   EXCLUDED.data_type
      |,  EXCLUDED.nullable
      |,  EXCLUDED.max_length
      |,  EXCLUDED.precision
      |,  EXCLUDED.scale
      |,  EXCLUDED.pk_cols
      |) 
      |IS DISTINCT FROM 
      |(
      |   $cacheSchema.table_def.data_type
      |,  $cacheSchema.table_def.nullable
      |,  $cacheSchema.table_def.max_length
      |,  $cacheSchema.table_def.precision
      |,  $cacheSchema.table_def.scale
      |,  $cacheSchema.table_def.pk_cols
      |) 
    """.trimMargin()

    if (showSQL) {
      val params = table.fields.mapIndexed { ix, field ->
        "[" + listOf(schema ?: "", table.name, field.name, ix) + "]"
      }.joinToString(", ")

      println(
        """
        |${javaClass.simpleName}.addTable insertFieldSQL - add field to table_def: 
        |$sql
        |PARAMS:
        |  $params
      """.trimMargin()
      )
    }

    con.prepareStatement(sql).use { preparedStatement ->
      table.fields.forEach { field ->
        val maxLength: Int? = when (field.dataType) {
          is DataType.text -> field.dataType.maxLength
          is DataType.nullableText -> field.dataType.maxLength
          else -> null
        }

        val (precision: Int?, scale: Int?) = when (field.dataType) {
          is DataType.decimal -> {
            field.dataType.precision to field.dataType.scale
          }
          is DataType.nullableDecimal -> {
            field.dataType.precision to field.dataType.scale
          }
          else -> {
            null to null
          }
        }

        val pkCols = con.createArrayOf("text", table.primaryKeyFieldNames.toTypedArray())

        preparedStatement.setString(1, schema ?: "")
        preparedStatement.setString(2, table.name)
        preparedStatement.setString(3, field.name)
        preparedStatement.setString(4, field.dataType.name)
        preparedStatement.setBoolean(5, field.dataType.nullable)
        if (maxLength == null) {
          preparedStatement.setNull(6, Types.INTEGER)
        } else {
          preparedStatement.setInt(6, maxLength)
        }
        if (precision == null) {
          preparedStatement.setNull(7, Types.INTEGER)
        } else {
          preparedStatement.setInt(7, precision)
        }
        if (scale == null) {
          preparedStatement.setNull(8, Types.INTEGER)
        } else {
          preparedStatement.setInt(8, scale)
        }
        preparedStatement.setArray(9, pkCols)

        preparedStatement.addBatch()
      }
      preparedStatement.executeBatch()
    }
  }

  override fun getTable(schema: String?, table: String): Table? {
    if (cache.containsKey(schema to table)) {
      return cache[schema to table]
    }

    //language=PostgreSQL
    val sql = """
      |SELECT 
      |   t.field_name
      |,  t.data_type
      |,  t.nullable
      |,  t.max_length
      |,  t.precision
      |,  t.scale
      |,  t.pk_cols
      |FROM $cacheSchema.table_def AS t
      |WHERE
      |   t.schema_name = ?
      |   AND t.table_name = ?
      |ORDER BY 
      |   t.field_name
    """.trimMargin()

    if (showSQL) {
      println(
        """
        |${javaClass.simpleName}.getTable - get fields from table_def:
        |$sql
        |PARAMS:
        |  schema_name: $schema
        |  table_name: $table
      """.trimMargin()
      )
    }

    val fields = mutableListOf<Field<*>>()
    val pkCols = mutableListOf<String>()
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.setString(1, schema ?: "")
      preparedStatement.setString(2, table)

      preparedStatement.executeQuery().use { rs ->
        while (rs.next()) {
          val fieldName = rs.getString("field_name")
          val dataTypeName = rs.getString("data_type")
          val maxLength = rs.getObject("max_length") as Int?
          val precision = rs.getObject("precision") as Int? ?: 18
          val scale = rs.getObject("scale") as Int? ?: 4
          if (pkCols.isEmpty()) {
            @Suppress("UNCHECKED_CAST")
            pkCols.addAll(rs.getArray("pk_cols").array as Array<out String>)
          }

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

          fields.add(Field(name = fieldName, dataType = dataType))
        }
      }
    }

    return if (fields.isEmpty()) {
      null
    } else {
      if (pkCols.isEmpty()) {
        throw KDAError.TableMissingAPrimaryKey(schema = schema, table = table)
      } else {
        Table(
          name = table,
          fields = fields.toSet(),
          primaryKeyFieldNames = pkCols,
        )
      }
    }
  }
}
