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

@Suppress("UNCHECKED_CAST")
internal fun ResultSet.toMap(fields: Set<Field<*>>): Row = Row(
  fields.associate { fld ->
    try {
      fld.name to when (fld.dataType) {
        DataType.nullableBigInt -> getObject(fld.name)
        DataType.nullableBool -> getObject(fld.name)
        is DataType.nullableDecimal -> getObject(fld.name)
        DataType.nullableFloat -> getObject(fld.name)
        DataType.nullableInt -> getObject(fld.name)
        is DataType.nullableText -> getObject(fld.name)
        DataType.nullableLocalDate -> (getObject(fld.name) as Date?)?.toLocalDate()
        DataType.nullableLocalDateTime -> (getObject(fld.name) as Timestamp?)?.toLocalDateTime()
        DataType.bigInt -> getLong(fld.name)
        DataType.bool -> getBoolean(fld.name)
        is DataType.decimal -> getBigDecimal(fld.name)
        DataType.float -> getFloat(fld.name)
        DataType.int -> getInt(fld.name)
        DataType.localDate -> getDate(fld.name).toLocalDate()
        DataType.localDateTime -> getTimestamp(fld.name).toLocalDateTime()
        is DataType.text -> getString(fld.name)
      }
    } catch (e: Throwable) {
      println("toMap() fld: $fld, sqlType: ${fld.dataType.description}")
      throw e
    }
  }
)
