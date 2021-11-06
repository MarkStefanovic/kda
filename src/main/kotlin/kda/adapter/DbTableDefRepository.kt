package kda.adapter

import kda.domain.DataType
import kda.domain.DataTypeName
import kda.domain.Field
import kda.domain.Table
import kda.domain.TableDefRepository
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select

class DbTableDefRepository(private val maxFloatDigits: Int = 5) : TableDefRepository {
  override fun add(table: Table) {
    table.primaryKeyFieldNames.forEachIndexed { ix, pk ->
      PrimaryKeys.insert {
        it[PrimaryKeys.schema] = table.schema ?: ""
        it[PrimaryKeys.table] = table.name
        it[PrimaryKeys.fieldName] = pk
        it[PrimaryKeys.order] = ix + 1
      }
    }
    table.fields.forEach { field ->
      TableDefs.insert {
        it[TableDefs.schema] = table.schema ?: ""
        it[TableDefs.table] = table.name
        it[TableDefs.column] = field.name
        it[TableDefs.dataType] = field.dataType.name
        when (field.dataType) {
          DataType.bool -> {}
          is DataType.decimal -> {
            it[TableDefs.precision] = field.dataType.precision
            it[TableDefs.scale] = field.dataType.scale
          }
          is DataType.float -> {}
          is DataType.int -> {
            it[TableDefs.autoincrement] = field.dataType.autoincrement
          }
          DataType.localDate -> {}
          is DataType.text -> {
            it[TableDefs.maxLength] = field.dataType.maxLength
          }
          DataType.localDateTime -> {}
          DataType.nullableBool -> {}
          is DataType.nullableDecimal -> {
            it[TableDefs.precision] = field.dataType.precision
            it[TableDefs.scale] = field.dataType.scale
          }
          is DataType.nullableFloat -> {}
          is DataType.nullableInt -> {
            it[TableDefs.autoincrement] = field.dataType.autoincrement
          }
          DataType.nullableLocalDate -> {}
          DataType.nullableLocalDateTime -> {}
          is DataType.nullableText -> {
            it[TableDefs.maxLength] = field.dataType.maxLength
          }
        }
      }
    }
  }

  override fun delete(schema: String?, table: String) {
    PrimaryKeys.deleteWhere {
      if (schema == null) {
        PrimaryKeys.table eq table
      } else {
        (PrimaryKeys.schema eq schema) and (PrimaryKeys.table eq table)
      }
    }
    TableDefs.deleteWhere {
      if (schema == null) {
        TableDefs.table eq table
      } else {
        (TableDefs.schema eq schema) and (TableDefs.table eq table)
      }
    }
  }

  override fun get(schema: String?, table: String): Table? {
    val fields =
      TableDefs
        .select {
          if (schema == null) {
            TableDefs.table eq table
          } else {
            (TableDefs.schema eq schema) and (TableDefs.table eq table)
          }
        }
        .map { row ->
          val dtype =
            when (row[TableDefs.dataType]) {
              DataTypeName.Bool -> DataType.bool
              DataTypeName.Decimal ->
                DataType.decimal(
                  precision = row[TableDefs.precision]
                    ?: error("precision is required"),
                  scale = row[TableDefs.scale] ?: error("scale is required"),
                )
              DataTypeName.Float -> DataType.float(maxFloatDigits)
              DataTypeName.Int -> DataType.int(row[TableDefs.autoincrement] ?: error("autoincrement is required"))
              DataTypeName.Date -> DataType.localDate
              DataTypeName.DateTime -> DataType.localDateTime
              DataTypeName.Text -> DataType.text(row[TableDefs.maxLength])
              DataTypeName.NullableBool -> DataType.nullableBool
              DataTypeName.NullableDecimal ->
                DataType.nullableDecimal(
                  precision = row[TableDefs.precision]
                    ?: error("precision is required"),
                  scale = row[TableDefs.scale] ?: error("scale is required"),
                )
              DataTypeName.NullableFloat -> DataType.nullableFloat(maxFloatDigits)
              DataTypeName.NullableInt ->
                DataType.nullableInt(row[TableDefs.autoincrement] ?: error("autoincrement is required"))
              DataTypeName.NullableDate -> DataType.nullableLocalDate
              DataTypeName.NullableDateTime -> DataType.nullableLocalDateTime
              DataTypeName.NullableText -> DataType.nullableText(row[TableDefs.maxLength])
            }
          Field(name = row[TableDefs.column], dataType = dtype)
        }
        .toSet()

    return if (fields.isEmpty()) {
      null
    } else {
      val primaryKeyFieldNames: List<String> =
        PrimaryKeys
          .select {
            if (schema == null) {
              PrimaryKeys.table eq table
            } else {
              (PrimaryKeys.schema eq schema) and (PrimaryKeys.table eq table)
            }
          }
          .orderBy(PrimaryKeys.order to SortOrder.ASC)
          .map { it[PrimaryKeys.fieldName] }

      Table(
        schema = schema,
        name = table,
        fields = fields,
        primaryKeyFieldNames = primaryKeyFieldNames,
      )
    }
  }
}
