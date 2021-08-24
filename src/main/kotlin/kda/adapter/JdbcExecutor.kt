package kda.adapter

import kda.domain.*
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.LocalDateTime

class JdbcExecutor(private val con: Connection) : SQLExecutor {
  override fun execute(sql: String) {
    with(con) { createStatement().use { stmt -> stmt.execute(sql) } }
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

  override fun fetchNullableDecimal(sql: String): BigDecimal? =
      fetchScalar(sql = sql, dataType = NullableDecimalType(precision = 19, scale = 2))

  override fun fetchDecimal(sql: String): BigDecimal =
      fetchScalar(sql = sql, dataType = DecimalType(precision = 19, scale = 2))

  override fun fetchNullableFloat(sql: String): Float? =
      fetchScalar(sql = sql, dataType = NullableFloatType(maxDigits = 5))

  override fun fetchFloat(sql: String): Float =
      fetchScalar(sql = sql, dataType = FloatType(maxDigits = 5))

  override fun fetchNullableInt(sql: String): Int? =
      fetchScalar(sql = sql, dataType = NullableIntType(autoincrement = false))

  override fun fetchInt(sql: String): Int =
      fetchScalar(sql = sql, dataType = IntType(autoincrement = false))

  override fun fetchNullableString(sql: String): String? =
      fetchScalar(sql = sql, dataType = NullableStringType(maxLength = null))

  override fun fetchString(sql: String): String =
      fetchScalar(sql = sql, dataType = StringType(maxLength = null))

  override fun fetchRow(sql: String, fields: Set<Field>): Row =
      con.createStatement().use { stmt ->
        stmt.executeQuery(sql).use { rs ->
          rs.next()
          Row(rs.toMap(fields = fields))
        }
      }

  override fun fetchRows(sql: String, fields: Set<Field>): Set<Row> =
      con.createStatement().use { stmt ->
        val rows: MutableList<Row> = mutableListOf()
        stmt.executeQuery(sql).use { rs ->
          while (rs.next()) {
            rows.add(Row(rs.toMap(fields = fields)))
          }
        }
        return rows.toSet()
      }

  private inline fun <reified Out : Any?> fetchScalar(sql: String, dataType: DataType<Out>): Out =
      con.createStatement().use { stmt ->
        stmt.executeQuery(sql).use { rs ->
          rs.next()
          val value =
              try {
                rs.getObject(1)
              } catch (e: Exception) {
                if (rs.metaData.columnCount == 0) throw NoRowsReturned(sql = sql) else throw e
              }

          try {
            when (value) {
              null ->
                  if (dataType.nullable) null
                  else throw NullValueError(expectedType = dataType::class.simpleName ?: "Unknown")
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
            throw ValueError(value = value, expectedType = Out::class.simpleName ?: "Unknown")
          }
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
                    scale = fld.dataType.scale)
            is NullableDecimalType ->
                NullableDecimalValue(
                    value = getObject(fld.name) as BigDecimal?,
                    precision = fld.dataType.precision,
                    scale = fld.dataType.scale)
            is FloatType ->
                FloatValue(value = getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
            is NullableFloatType ->
                FloatValue(value = getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
            is IntType -> IntValue(getInt(fld.name))
            is NullableIntType -> NullableIntValue(getObject(fld.name) as Int?)
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
                StringValue(value = getString(fld.name), maxLength = fld.dataType.maxLength)
          }
    }

// private fun rsToRow(fields: Set<Field>, rs: ResultSet): Row {
//    val valueMap: MutableMap<String, Value<*>> = mutableMapOf()
//    for (fld in fields) {
//        valueMap[fld.name] =
//            when (fld.dataType) {
//                BoolType -> BoolValue(value = rs.getBoolean(fld.name))
//                NullableBoolType -> NullableBoolValue(value = rs.getObject(fld.name) as Boolean?)
//                is DecimalType ->
//                    DecimalValue(
//                        value = rs.getBigDecimal(fld.name),
//                        precision = fld.dataType.precision,
//                        scale = fld.dataType.scale
//                    )
//                is NullableDecimalType ->
//                    NullableDecimalValue(
//                        value = rs.getObject(fld.name) as BigDecimal?,
//                        precision = fld.dataType.precision,
//                        scale = fld.dataType.scale
//                    )
//                is FloatType ->
//                    FloatValue(value = rs.getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
//                is NullableFloatType ->
//                    FloatValue(value = rs.getFloat(fld.name), maxDigits = fld.dataType.maxDigits)
//                is IntType -> IntValue(rs.getInt(fld.name))
//                is NullableIntType -> NullableIntValue(rs.getObject(fld.name) as Int?)
//                LocalDateTimeType ->
//                    LocalDateTimeValue(value = rs.getTimestamp(fld.name).toLocalDateTime())
//                NullableLocalDateTimeType ->
//                    NullableLocalDateTimeValue(value =
// rs.getTimestamp(fld.name)?.toLocalDateTime())
//                LocalDateType -> LocalDateValue(value = rs.getDate(fld.name).toLocalDate())
//                NullableLocalDateType ->
//                    NullableLocalDateValue(value = rs.getDate(fld.name)?.toLocalDate())
//                is StringType ->
//                    StringValue(value = rs.getString(fld.name), maxLength =
// fld.dataType.maxLength)
//                is NullableStringType ->
//                    StringValue(value = rs.getString(fld.name), maxLength =
// fld.dataType.maxLength)
//            }
//    }
//    return Row(valueMap)
// }
