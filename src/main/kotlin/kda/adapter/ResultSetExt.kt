package kda.adapter

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Row
import java.sql.Date
import java.sql.ResultSet
import java.sql.Timestamp

fun ResultSet.toRows(fields: Set<Field<*>>): Sequence<Row> = sequence {
  while (next()) {
    yield(toMap(fields))
  }
}

internal fun ResultSet.toMap(fields: Set<Field<*>>): Row = Row(
  fields.associate { fld ->
    try {
      fld.name to getValue(fieldName = fld.name, dataType = fld.dataType)
    } catch (e: Throwable) {
      println("toMap() fld: $fld, sqlType: ${fld.dataType.description}")
      throw e
    }
  }
)

@Suppress("UNCHECKED_CAST")
internal fun <T : Any?> ResultSet.getValue(fieldName: String, dataType: DataType<T>): T =
  when (dataType) {
    DataType.nullableBigInt -> getObject(fieldName)
    DataType.nullableBool -> getObject(fieldName)
    is DataType.nullableDecimal -> getObject(fieldName)
    DataType.nullableFloat -> getObject(fieldName)
    DataType.nullableInt -> getObject(fieldName)
    is DataType.nullableText -> getObject(fieldName)
    DataType.nullableLocalDate -> (getObject(fieldName) as Date?)?.toLocalDate()
    DataType.nullableLocalDateTime -> (getObject(fieldName) as Timestamp?)?.toLocalDateTime()
    DataType.bigInt -> getLong(fieldName)
    DataType.bool -> getBoolean(fieldName)
    is DataType.decimal -> getBigDecimal(fieldName)
    DataType.float -> getFloat(fieldName)
    DataType.int -> getInt(fieldName)
    DataType.localDate -> getDate(fieldName).toLocalDate()
    DataType.localDateTime -> getTimestamp(fieldName).toLocalDateTime()
    is DataType.text -> getString(fieldName)
  } as T
