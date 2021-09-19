package kda.adapter

import kda.domain.BoolType
import kda.domain.DataTypeName
import kda.domain.DecimalType
import kda.domain.Field
import kda.domain.FloatType
import kda.domain.IntType
import kda.domain.LocalDateTimeType
import kda.domain.LocalDateType
import kda.domain.NullableBoolType
import kda.domain.NullableDecimalType
import kda.domain.NullableFloatType
import kda.domain.NullableIntType
import kda.domain.NullableLocalDateTimeType
import kda.domain.NullableLocalDateType
import kda.domain.NullableStringType
import kda.domain.StringType
import kda.domain.Table
import kda.domain.TableDefRepository
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select

class DbTableDefRepository(
  private val db: Db,
  private val maxFloatDigits: Int = 5,
) : TableDefRepository {
  override fun add(table: Table) {
    db.exec {
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
            BoolType -> {}
            is DecimalType -> {
              it[TableDefs.precision] = field.dataType.precision
              it[TableDefs.scale] = field.dataType.scale
            }
            is FloatType -> {}
            is IntType -> {
              it[TableDefs.autoincrement] = field.dataType.autoincrement
            }
            LocalDateTimeType -> {}
            LocalDateType -> {}
            NullableBoolType -> {}
            is NullableDecimalType -> {
              it[TableDefs.precision] = field.dataType.precision
              it[TableDefs.scale] = field.dataType.scale
            }
            is NullableFloatType -> {}
            is NullableIntType -> {
              it[TableDefs.autoincrement] = field.dataType.autoincrement
            }
            NullableLocalDateTimeType -> {}
            NullableLocalDateType -> {}
            is NullableStringType -> {
              it[TableDefs.maxLength] = field.dataType.maxLength
            }
            is StringType -> {
              it[TableDefs.maxLength] = field.dataType.maxLength
            }
          }
        }
      }
    }
  }

  override fun delete(schema: String?, table: String) {
    db.exec {
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
  }

  override fun get(schema: String?, table: String): Table? {
    val fields =
      db.fetch {
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
                DataTypeName.Bool -> BoolType
                DataTypeName.Decimal ->
                  DecimalType(
                    precision = row[TableDefs.precision]
                      ?: error("precision is required"),
                    scale = row[TableDefs.scale] ?: error("scale is required"),
                  )
                DataTypeName.Float -> FloatType(maxFloatDigits)
                DataTypeName.Int -> IntType(row[TableDefs.autoincrement] ?: error("autoincrement is required"))
                DataTypeName.Date -> LocalDateType
                DataTypeName.DateTime -> LocalDateTimeType
                DataTypeName.Text -> StringType(row[TableDefs.maxLength])
                DataTypeName.NullableBool -> NullableBoolType
                DataTypeName.NullableDecimal ->
                  NullableDecimalType(
                    precision = row[TableDefs.precision]
                      ?: error("precision is required"),
                    scale = row[TableDefs.scale] ?: error("scale is required"),
                  )
                DataTypeName.NullableFloat -> NullableFloatType(maxFloatDigits)
                DataTypeName.NullableInt ->
                  NullableIntType(row[TableDefs.autoincrement] ?: error("autoincrement is required"))
                DataTypeName.NullableDate -> NullableLocalDateType
                DataTypeName.NullableDateTime -> NullableLocalDateTimeType
                DataTypeName.NullableText -> NullableStringType(row[TableDefs.maxLength])
              }
            Field(name = row[TableDefs.column], dataType = dtype)
          }
          .toSet()
      }
    return if (fields.isEmpty()) {
      null
    } else {
      val primaryKeyFieldNames: List<String> =
        db.fetch {
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
        }
      Table(
        schema = schema,
        name = table,
        fields = fields,
        primaryKeyFieldNames = primaryKeyFieldNames,
      )
    }
  }
}
