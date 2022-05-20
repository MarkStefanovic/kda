package kda.adapter

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Row
import java.sql.Date
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit

fun ResultSet.toRows(fields: Set<Field<*>>, timestampResolution: ChronoUnit): Sequence<Row> = sequence {
  while (next()) {
    yield(toMap(fields, timestampResolution = timestampResolution))
  }
}

internal fun ResultSet.toMap(fields: Set<Field<*>>, timestampResolution: ChronoUnit): Row = Row(
  fields.associate { fld ->
    try {
      fld.name to getValue(fieldName = fld.name, dataType = fld.dataType, timestampResolution = timestampResolution)
    } catch (e: Throwable) {
      println("toMap() fld: $fld, sqlType: ${fld.dataType.description}")
      throw e
    }
  }
)

@Suppress("UNCHECKED_CAST")
internal fun <T : Any?> ResultSet.getValue(
  fieldName: String,
  dataType: DataType<T>,
  timestampResolution: ChronoUnit,
): T =
  when (dataType) {
    DataType.nullableBigInt -> getObject(fieldName)
    DataType.nullableBool -> getObject(fieldName)
    is DataType.nullableDecimal -> getObject(fieldName)
    DataType.nullableFloat -> getObject(fieldName)
    DataType.nullableInt -> getObject(fieldName)
    is DataType.nullableText -> getObject(fieldName)
    DataType.nullableLocalDate -> (getObject(fieldName) as Date?)?.toLocalDate()
    is DataType.nullableTimestamp -> (getObject(fieldName) as Timestamp?)?.toLocalDateTime()?.truncatedTo(timestampResolution)
    is DataType.nullableTimestampUTC -> (getObject(fieldName) as OffsetDateTime?)?.truncatedTo(timestampResolution)
    DataType.bigInt -> getLong(fieldName)
    DataType.bool -> getBoolean(fieldName)
    is DataType.decimal -> getBigDecimal(fieldName)
    DataType.float -> getFloat(fieldName)
    DataType.int -> getInt(fieldName)
    DataType.localDate -> getDate(fieldName).toLocalDate()
    is DataType.timestamp -> getTimestamp(fieldName).toLocalDateTime().truncatedTo(timestampResolution)
    is DataType.text -> getString(fieldName)
    is DataType.timestampUTC -> (getObject(fieldName) as OffsetDateTime).truncatedTo(timestampResolution)
  } as T
