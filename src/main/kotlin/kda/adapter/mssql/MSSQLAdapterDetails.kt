package kda.adapter.mssql

import kda.adapter.std.StdAdapterDetails
import kda.domain.DataType

@ExperimentalStdlibApi
object MSSQLAdapterDetails : StdAdapterDetails() {

  override fun wrapName(name: String): String = "[$name]"

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
