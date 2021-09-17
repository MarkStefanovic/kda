package kda.adapter

import kda.domain.*
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.LocalDateTime

class JdbcExecutor(private val con: Connection) : SQLExecutor {
  override fun execute(sql: String) {
    with(con) {
      try {
        createStatement().use { stmt -> stmt.execute(sql) }
      } catch (e: Exception) {
        //        println("The following error occurred while executing '$sql':")
        //        e.printStackTrace()
        //        throw e
        throw Exception(
          "The following error occurred while executing '$sql': ${e.stackTraceToString()}"
        )
      }
    }
  }

  override fun fetchNullableBool(sql: String): Boolean? =
    fetchScalar(sql = sql, dataType = NullableBoolType)

  override fun fetchBool(sql: String): Boolean = fetchScalar(sql = sql, dataType = BoolType)

  override fun fetchNullableDate(sql: String): LocalDate? =
    fetchScalar(sql = sql, dataType = NullableLocalDateType)

  override fun fetchDate(sql: String): LocalDate = fetchScalar(sql = sql, dataType = LocalDateType)

  override fun fetchNullableDateTime(sql: String): LocalDateTime? =
    fetchScalar(sql = sql, dataType = NullableLocalDateTimeType)

  override fun fetchDateTime(sql: String): LocalDateTime =
    fetchScalar(sql = sql, dataType = LocalDateTimeType)

  override fun fetchNullableDecimal(sql: String, precision: Int, scale: Int): BigDecimal? =
    fetchScalar(sql = sql, dataType = NullableDecimalType(precision = precision, scale = scale))

  override fun fetchDecimal(sql: String, precision: Int, scale: Int): BigDecimal =
    fetchScalar(sql = sql, dataType = DecimalType(precision = precision, scale = scale))

  override fun fetchNullableFloat(sql: String, maxDigits: Int): Float? =
    fetchScalar(sql = sql, dataType = NullableFloatType(maxDigits = maxDigits))

  override fun fetchFloat(sql: String, maxDigits: Int): Float =
    fetchScalar(sql = sql, dataType = FloatType(maxDigits = maxDigits))

  override fun fetchNullableInt(sql: String): Int? =
    fetchScalar(sql = sql, dataType = NullableIntType(autoincrement = false))

  override fun fetchInt(sql: String): Int =
    fetchScalar(sql = sql, dataType = IntType(autoincrement = false))

  override fun fetchNullableString(sql: String, maxLength: Int?): String? =
    fetchScalar(sql = sql, dataType = NullableStringType(maxLength = null))

  override fun fetchString(sql: String, maxLength: Int?): String =
    fetchScalar(sql = sql, dataType = StringType(maxLength = null))

  override fun fetchRow(sql: String, fields: Set<Field>): Row =
    con.createStatement().use { stmt ->
      stmt.executeQuery(sql).use { rs ->
        rs.next()
        Row(rs.toMap(fields))
      }
    }

  override fun fetchRows(sql: String, fields: Set<Field>): Set<Row> =
    con.createStatement().use { stmt ->
      val rows: MutableList<Row> = mutableListOf()
      try {
        stmt.executeQuery(sql).use { rs ->
          while (rs.next()) {
            rows.add(Row(rs.toMap(fields)))
          }
        }
      } catch (e: Exception) {
        throw Exception(
          "The following error occurred while executing '$sql': ${e.stackTraceToString()}"
        )
      }
      return rows.toSet()
    }

  private inline fun <reified Out : Any?> fetchScalar(sql: String, dataType: DataType<Out>): Out =
    try {
      con.createStatement().use { stmt ->
        stmt.executeQuery(sql).use { rs ->
          rs.next()
          if (rs.metaData.columnCount == 0) {
            throw KDAError.NoRowsReturned(sql = sql)
          } else {
            val value = rs.getObject(1)
            try {
              when (value) {
                null ->
                  if (dataType.nullable) null
                  else
                    throw KDAError.NullValueError(expectedType = dataType::class.simpleName ?: "Unknown")
                is Out -> value
                else -> {
                  when (dataType) {
                    BoolType, NullableBoolType -> value as Boolean
                    is DecimalType, is NullableDecimalType -> value as BigDecimal
                    is FloatType, is NullableFloatType -> (value as Double).toFloat()
                    is IntType, is NullableIntType -> if (value is Long) value.toInt() else value
                    LocalDateTimeType, NullableLocalDateTimeType ->
                      (value as Timestamp).toLocalDateTime()
                    LocalDateType, NullableLocalDateType -> (value as Date).toLocalDate()
                    is StringType, is NullableStringType -> value as String
                  }
                }
              } as
                Out
            } catch (e: ClassCastException) {
              throw KDAError.ValueError(value = value, expectedType = Out::class.simpleName ?: "Unknown")
            }
          }
        }
      }
    } catch (e: Exception) {
      if (e is KDAError) {
        throw e
      } else {
        throw KDAError.SQLError(sql = sql, originalError = e)
      }
    }
}

private fun ResultSet.toMap(fields: Set<Field>): Map<String, Value<*>> =
  fields.associate { fld ->
    fld.name to
      when (fld.dataType) {
        BoolType -> BoolValue(value = getBoolean(fld.name))
        NullableBoolType -> NullableBoolValue(value = getObject(fld.name) as Boolean?)
        is DecimalType ->
          DecimalValue(
            value = getBigDecimal(fld.name),
            precision = fld.dataType.precision,
            scale = fld.dataType.scale
          )
        is NullableDecimalType ->
          NullableDecimalValue(
            value = getObject(fld.name) as BigDecimal?,
            precision = fld.dataType.precision,
            scale = fld.dataType.scale
          )
        is FloatType ->
          FloatValue(value = getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
        is NullableFloatType ->
          NullableFloatValue(
            value = getObject(fld.name) as? Float?, maxDigits = fld.dataType.maxDigits
          )
        is IntType -> IntValue(getLong(fld.name).toInt())
        is NullableIntType -> {
          if (getObject(fld.name) == null) {
            NullableIntValue(null)
          } else {
            IntValue(getLong(fld.name).toInt())
          }
        }
        LocalDateTimeType ->
          LocalDateTimeValue(value = getTimestamp(fld.name).toLocalDateTime())
        NullableLocalDateTimeType ->
          NullableLocalDateTimeValue(value = getTimestamp(fld.name)?.toLocalDateTime())
        LocalDateType -> LocalDateValue(value = getDate(fld.name).toLocalDate())
        NullableLocalDateType ->
          NullableLocalDateValue(value = getDate(fld.name)?.toLocalDate())
        is StringType ->
          StringValue(value = getString(fld.name), maxLength = fld.dataType.maxLength)
        is NullableStringType ->
          NullableStringValue(
            value = getObject(fld.name) as? String?, maxLength = fld.dataType.maxLength
          )
      }
  }
