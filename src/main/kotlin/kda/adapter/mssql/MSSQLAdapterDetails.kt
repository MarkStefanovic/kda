package kda.adapter.mssql

import kda.adapter.std.StdAdapterDetails
import kda.domain.DataType
import kda.domain.Field

@ExperimentalStdlibApi
object MSSQLAdapterDetails : StdAdapterDetails() {

  override fun wrapName(name: String): String = "[$name]"

  override fun castField(field: Field<*>): String {
    val wrappedName = wrapName(field.name)

    return when (val dataType = field.dataType) {
      DataType.bigInt, DataType.nullableBigInt -> "CAST($wrappedName AS BIGINT)"
      DataType.bool, DataType.nullableBool -> "CAST($wrappedName AS BOOLEAN)"
      is DataType.decimal -> "CAST(? AS NUMERIC(${dataType.precision}, ${dataType.scale})"
      is DataType.nullableDecimal -> "CAST(? AS NUMERIC(${dataType.precision}, ${dataType.scale})"
      DataType.float, DataType.nullableFloat -> "CAST($wrappedName AS FLOAT)"
      DataType.int, DataType.nullableInt -> "CAST($wrappedName AS INT)"
      DataType.localDate, DataType.nullableLocalDate -> "CAST($wrappedName AS DATE)"
      is DataType.timestamp -> "CAST($wrappedName AS DATETIME)"
      is DataType.nullableTimestamp -> "CAST($wrappedName AS DATETIME)"
      is DataType.nullableText -> if (dataType.maxLength == null) "CAST($wrappedName AS TEXT)" else "CAST($wrappedName AS VARCHAR(${dataType.maxLength})"
      is DataType.text -> if (dataType.maxLength == null) "CAST($wrappedName AS TEXT)" else "CAST($wrappedName AS VARCHAR(${dataType.maxLength})"
      is DataType.timestampUTC -> "CAST($wrappedName AS DATETIMEOFFSET(0))"
      is DataType.nullableTimestampUTC -> "CAST($wrappedName AS DATETIMEOFFSET(0))"
    }
  }

  override fun castParameter(dataType: DataType<*>): String = when (dataType) {
    DataType.bigInt, DataType.nullableBigInt -> "CAST(? AS BIGINT)"
    DataType.bool, DataType.nullableBool -> "CAST(? AS BOOLEAN)"
    is DataType.decimal -> "CAST(? AS NUMERIC(${dataType.precision}, ${dataType.scale})"
    is DataType.nullableDecimal -> "CAST(? AS NUMERIC(${dataType.precision}, ${dataType.scale})"
    DataType.float, DataType.nullableFloat -> "CAST(? AS FLOAT)"
    DataType.int, DataType.nullableInt -> "CAST(? AS INT)"
    DataType.localDate, DataType.nullableLocalDate -> "CAST(? AS DATE)"
    is DataType.timestamp -> "CAST(? AS DATETIME)"
    is DataType.nullableTimestamp -> "CAST(? AS DATETIME)"
    is DataType.nullableText -> if (dataType.maxLength == null) "CAST(? AS TEXT)" else "CAST(? AS VARCHAR(${dataType.maxLength})"
    is DataType.text -> if (dataType.maxLength == null) "CAST(? AS TEXT)" else "CAST(? AS VARCHAR(${dataType.maxLength})"
    is DataType.timestampUTC -> "CAST(? AS DATETIMEOFFSET(0))"
    is DataType.nullableTimestampUTC -> "CAST(? AS DATETIMEOFFSET(0))"
  }
}
