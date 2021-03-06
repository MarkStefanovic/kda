package kda.adapter

import kda.domain.BoundParameter
import kda.domain.DataType
import kda.domain.KDAError
import kda.domain.Parameter
import kda.domain.Row
import java.math.BigDecimal
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime

internal fun PreparedStatement.applyValue(
  dataType: DataType<*>,
  index: Int,
  value: Any?,
) {
  if (value == null) {
    setNull(index, dataType.jdbcType.vendorTypeNumber)
  } else {
    when (dataType) {
      DataType.bigInt, DataType.nullableBigInt -> setLong(index, (value as Number).toLong())
      DataType.bool, DataType.nullableBool -> setBoolean(index, value as Boolean)
      is DataType.decimal, is DataType.nullableDecimal -> setBigDecimal(index, value as BigDecimal)
      DataType.float, DataType.nullableFloat -> setFloat(index, value as Float)
      DataType.int, DataType.nullableInt -> setInt(index, (value as Number).toInt())
      DataType.localDate, DataType.nullableLocalDate -> setDate(index, Date.valueOf(value as LocalDate))
      is DataType.timestamp, is DataType.nullableTimestamp -> setTimestamp(index, Timestamp.valueOf(value as LocalDateTime))
      is DataType.text, is DataType.nullableText -> setString(index, value as String)
      is DataType.timestampUTC, is DataType.nullableTimestampUTC -> setObject(index, value)
    }
  }
}

internal fun PreparedStatement.applyRow(row: Row, parameters: Collection<Parameter>) {
  parameters.forEachIndexed { index, parameter ->
    if (!row.value.containsKey(parameter.name)) {
      throw KDAError.FieldNotFound(
        fieldName = parameter.name,
        availableFieldNames = row.fields,
      )
    }
    applyValue(
      dataType = parameter.dataType,
      index = index + 1,
      value = row.value[parameter.name],
    )
  }
}

internal fun PreparedStatement.applyBoundParameters(parameters: Collection<BoundParameter>) {
  parameters.forEachIndexed { index, boundParameter ->
    try {
      applyValue(
        dataType = boundParameter.parameter.dataType,
        index = index + 1,
        value = boundParameter.value,
      )
    } catch (e: Throwable) {
      println(
        """
        |PreparedStatement.applyBoundParameters: 
        |  name: ${boundParameter.parameter.name}
        |  index: ${index + 1}
        |  parameters: ${boundParameter.value}
        |  preparedStatement: $this
        """.trimMargin()
      )
      throw e
    }
  }
}
