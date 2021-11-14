package kda.adapter

import kda.domain.Criteria
import kda.domain.DataType
import kda.domain.DataTypeName
import kda.domain.Datasource
import kda.domain.Field
import kda.domain.Operator
import kda.domain.Predicate
import kda.domain.Row
import kda.domain.Table
import kda.domain.TableDefRepository
import java.time.LocalDateTime

class DbTableDefRepository(
  private val ds: Datasource,
  private val showSQL: Boolean,
  private val maxFloatDigits: Int,
) : TableDefRepository {

  override fun add(table: Table) {
    val pkRows =
      table
        .primaryKeyFieldNames
        .mapIndexed { ix, pk ->
          primaryKeys.row(
            "schema_name" to table.schema,
            "table_name" to table.name,
            "field_name" to pk,
            "order" to ix + 1,
            "date_added" to LocalDateTime.now(),
          )
        }
        .toSet()

    val insertPKRowsSQL = ds.adapter.add(table = primaryKeys, rows = pkRows)

    if (showSQL) {
      println(insertPKRowsSQL)
    }

    ds.executor.execute(insertPKRowsSQL)

    val fieldRows =
      table
        .fields
        .map { fld ->
          val dataTypeFields: Map<String, Any?> = when (fld.dataType) {
            DataType.bool -> emptyMap()
            is DataType.decimal -> mapOf(
              "precision" to fld.dataType.precision,
              "scale" to fld.dataType.precision,
            )
            is DataType.float -> emptyMap()
            is DataType.int -> mapOf(
              "autoincrement" to fld.dataType.autoincrement
            )
            DataType.localDate -> emptyMap()
            DataType.localDateTime -> emptyMap()
            DataType.nullableBool -> emptyMap()
            is DataType.nullableDecimal -> mapOf(
              "precision" to fld.dataType.precision,
              "scale" to fld.dataType.precision,
            )
            is DataType.nullableFloat -> emptyMap()
            is DataType.nullableInt -> mapOf(
              "autoincrement" to fld.dataType.autoincrement
            )
            DataType.nullableLocalDate -> emptyMap()
            DataType.nullableLocalDateTime -> emptyMap()
            is DataType.nullableText -> mapOf(
              "max_length" to fld.dataType.maxLength
            )
            is DataType.text -> mapOf(
              "max_length" to fld.dataType.maxLength
            )
          }

          val rowMap = mapOf(
            "schema_name" to table.schema,
            "table_name" to table.name,
            "field_name" to fld.name,
            "data_type" to fld.dataType.name.name,
            "date_added" to LocalDateTime.now(),
            "autoincrement" to false,
            "max_length" to null,
            "precision" to null,
            "scale" to null,
          ) + dataTypeFields

          tableDefs.row(rowMap)
        }
        .toSet()

    val insertFieldsSQL = ds.adapter.add(table = tableDefs, rows = fieldRows)

    if (showSQL) {
      println(insertFieldsSQL)
    }

    ds.executor.execute(insertFieldsSQL)
  }

  override fun delete(schema: String?, table: String) {
    val primaryKeyRows = setOf(
      primaryKeys.row(
        "schema_name" to schema,
        "table_name" to table,
      )
    )

    val deletePrimaryKeySQL = ds.adapter.delete(
      table = primaryKeys,
      rows = primaryKeyRows,
    )

    if (showSQL) {
      println(deletePrimaryKeySQL)
    }

    ds.executor.execute(deletePrimaryKeySQL)

    val deleteTableDefSQL = ds.adapter.delete(
      table = tableDefs,
      rows = primaryKeyRows,
    )

    if (showSQL) {
      println(deleteTableDefSQL)
    }

    ds.executor.execute(deleteTableDefSQL)
  }

  override fun get(schema: String?, table: String): Table? {
    val tableDefRows = getTableDefRows(schema = schema, table = table)

    return if (tableDefRows.isEmpty()) {
      null
    } else {
      val pkFieldNames = getPrimaryKeyFieldNames(schema = schema, table = table)

      val fields = tableDefRows
        .map { row ->
          val dataType = when (DataTypeName.valueOf(row.value("data_type").value as String)) {
            DataTypeName.Bool -> DataType.bool
            DataTypeName.Decimal -> DataType.decimal(
              precision = row.value("precision").value as Int,
              scale = row.value("scale").value as Int,
            )
            DataTypeName.Float -> DataType.float(maxFloatDigits)
            DataTypeName.Int -> DataType.int(
              autoincrement = row.value("autoincrement").value as Boolean
            )
            DataTypeName.Date -> DataType.localDate
            DataTypeName.DateTime -> DataType.localDateTime
            DataTypeName.Text -> DataType.text(
              maxLength = row.value("max_length").value as Int?
            )
            DataTypeName.NullableBool -> DataType.nullableBool
            DataTypeName.NullableDecimal -> DataType.nullableDecimal(
              precision = row.value("precision").value as Int,
              scale = row.value("scale").value as Int,
            )
            DataTypeName.NullableFloat -> DataType.nullableFloat(maxFloatDigits)
            DataTypeName.NullableInt -> DataType.nullableInt(
              autoincrement = row.value("autoincrement").value as Boolean
            )
            DataTypeName.NullableDate -> DataType.nullableLocalDate
            DataTypeName.NullableDateTime -> DataType.nullableLocalDateTime
            DataTypeName.NullableText -> DataType.nullableText(
              maxLength = row.value("max_length").value as Int?
            )
          }

          Field(name = row.value("field_name").value as String, dataType = dataType)
        }
        .toSet()

      if (fields.isEmpty()) {
        null
      } else {
        Table(
          schema = schema,
          name = table,
          fields = fields,
          primaryKeyFieldNames = pkFieldNames,
        )
      }
    }
  }

  @Suppress("DuplicatedCode")
  private fun getPrimaryKeyFieldNames(schema: String?, table: String): List<String> {
    val schemaField = primaryKeys.field("schema_name")

    val tableField = primaryKeys.field("table_name")

    val criteria = Criteria(
      setOf(
        setOf(
          Predicate(
            field = schemaField,
            value = schemaField.wrapValue(schema),
            operator = Operator.Equals,
          ),
          Predicate(
            field = tableField,
            value = tableField.wrapValue(table),
            operator = Operator.Equals,
          ),
        )
      )
    )

    val sql = ds.adapter.select(table = primaryKeys, criteria = criteria)

    if (showSQL) {
      println(sql)
    }

    val rows = ds.executor.fetchRows(sql = sql, fields = primaryKeys.fields)

    return rows
      .sortedBy { it.value("order").value as Int }
      .map { it.value("field_name").value as String }
      .toList()
  }

  @Suppress("DuplicatedCode")
  private fun getTableDefRows(schema: String?, table: String): Set<Row> {
    val schemaField = tableDefs.field("schema_name")

    val tableField = tableDefs.field("table_name")

    val criteria = Criteria(
      setOf(
        setOf(
          Predicate(
            field = schemaField,
            value = schemaField.wrapValue(schema),
            operator = Operator.Equals,
          ),
          Predicate(
            field = tableField,
            value = tableField.wrapValue(table),
            operator = Operator.Equals,
          ),
        )
      )
    )

    val sql = ds.adapter.select(table = tableDefs, criteria = criteria)

    if (showSQL) {
      println(sql)
    }

    return ds.executor.fetchRows(sql = sql, fields = tableDefs.fields).toSet()
  }
}
