package kda.adapter

import kda.domain.DataType
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.Row
import kda.domain.SQLExecutor
import kda.domain.Value
import java.io.Closeable
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime

class JdbcExecutor(private val con: Connection) : SQLExecutor, Closeable {
  override fun execute(sql: String) {
    try {
      con.createStatement().use { stmt -> stmt.execute(sql) }
    } catch (e: Exception) {
      throw Exception(
        "The following error occurred while executing '$sql': ${e.stackTraceToString()}"
      )
    }
  }

  override fun fetchNullableBool(sql: String): Boolean? =
    fetchScalar(sql = sql, dataType = DataType.nullableBool)

  override fun fetchBool(sql: String): Boolean = fetchScalar(sql = sql, dataType = DataType.bool)

  override fun fetchNullableDate(sql: String): LocalDate? =
    fetchScalar(sql = sql, dataType = DataType.nullableLocalDate)

  override fun fetchDate(sql: String): LocalDate = fetchScalar(sql = sql, dataType = DataType.localDate)

  override fun fetchNullableDateTime(sql: String): LocalDateTime? =
    fetchScalar(sql = sql, dataType = DataType.nullableLocalDateTime)

  override fun fetchDateTime(sql: String): LocalDateTime =
    fetchScalar(sql = sql, dataType = DataType.localDateTime)

  override fun fetchNullableDecimal(sql: String, precision: Int, scale: Int): BigDecimal? =
    fetchScalar(sql = sql, dataType = DataType.nullableDecimal(precision = precision, scale = scale))

  override fun fetchDecimal(sql: String, precision: Int, scale: Int): BigDecimal =
    fetchScalar(sql = sql, dataType = DataType.decimal(precision = precision, scale = scale))

  override fun fetchNullableFloat(sql: String, maxDigits: Int): Float? =
    fetchScalar(sql = sql, dataType = DataType.nullableFloat(maxDigits = maxDigits))

  override fun fetchFloat(sql: String, maxDigits: Int): Float =
    fetchScalar(sql = sql, dataType = DataType.float(maxDigits = maxDigits))

  override fun fetchNullableInt(sql: String): Int? =
    fetchScalar(sql = sql, dataType = DataType.nullableInt(autoincrement = false))

  override fun fetchInt(sql: String): Int =
    fetchScalar(sql = sql, dataType = DataType.int(autoincrement = false))

  override fun fetchNullableString(sql: String, maxLength: Int?): String? =
    fetchScalar(sql = sql, dataType = DataType.nullableText(maxLength = null))

  override fun fetchString(sql: String, maxLength: Int?): String =
    fetchScalar(sql = sql, dataType = DataType.text(maxLength = null))

  override fun fetchRow(sql: String, fields: Set<Field>): Row =
    con.createStatement().use { stmt ->
      stmt.executeQuery(sql).use { rs ->
        rs.next()
        Row(rs.toMap(fields))
      }
    }

  override fun fetchRows(sql: String, fields: Set<Field>): Sequence<Row> =
    sequence {
      con.createStatement().use { stmt ->
        try {
          stmt.executeQuery(sql).use { rs ->
            while (rs.next()) {
              yield(Row(rs.toMap(fields)))
            }
          }
        } catch (e: Exception) {
          throw Exception(
            "The following error occurred while executing '$sql': ${e.stackTraceToString()}"
          )
        }
      }
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
              val v = when (value) {
                null ->
                  if (dataType.nullable) {
                    null
                  } else {
                    throw KDAError.NullValueError(expectedType = dataType::class.simpleName ?: "Unknown")
                  }
                is Out -> value
                else -> {
                  when (dataType) {
                    DataType.bool, DataType.nullableBool -> value as Boolean
                    is DataType.decimal, is DataType.nullableDecimal -> value as BigDecimal
                    is DataType.float, is DataType.nullableFloat -> (value as Double).toFloat()
                    is DataType.int, is DataType.nullableInt -> if (value is Long) value.toInt() else value
                    DataType.localDateTime, DataType.nullableLocalDateTime ->
                      (value as Timestamp).toLocalDateTime()
                    DataType.localDate, DataType.nullableLocalDate -> (value as Date).toLocalDate()
                    is DataType.text, is DataType.nullableText -> value as String
                  }
                }
              }
              v as Out
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

  override fun close() {
    con.close()
  }
}

private fun ResultSet.toMap(fields: Set<Field>): Map<String, Value<*>> =
  fields.associate { fld ->
    fld.name to
      when (fld.dataType) {
        DataType.bool -> {
          val v = getObject(fld.name)
          if (v is Int) {
            if (v == 0) {
              Value.nullableBool(false)
            } else {
              Value.nullableBool(true)
            }
          } else {
            Value.nullableBool(v as Boolean)
          }
        }
        DataType.nullableBool -> {
          val v = getObject(fld.name)
          if (v == null) {
            Value.nullableBool(null)
          } else {
            if (v is Int) {
              if (v == 0) {
                Value.nullableBool(false)
              } else {
                Value.nullableBool(true)
              }
            } else {
              Value.nullableBool(v as Boolean)
            }
          }
        }
        is DataType.decimal ->
          Value.decimal(value = getBigDecimal(fld.name))
        is DataType.nullableDecimal ->
          Value.nullableDecimal(value = getObject(fld.name) as BigDecimal?)
        is DataType.float ->
          Value.float(value = getFloat(fld.name))
        is DataType.nullableFloat ->
          Value.nullableFloat(value = getObject(fld.name) as? Float?)
        is DataType.int ->
          Value.int(getLong(fld.name).toInt())
        is DataType.nullableInt -> {
          if (getObject(fld.name) == null) {
            Value.nullableInt(null)
          } else {
            Value.int(getLong(fld.name).toInt())
          }
        }
        DataType.localDateTime ->
          Value.datetime(value = getTimestamp(fld.name).toLocalDateTime())
        DataType.nullableLocalDateTime ->
          Value.nullableDatetime(value = getTimestamp(fld.name)?.toLocalDateTime())
        DataType.localDate ->
          Value.date(value = getDate(fld.name).toLocalDate())
        DataType.nullableLocalDate ->
          Value.nullableDate(value = getDate(fld.name)?.toLocalDate())
        is DataType.text ->
          Value.text(value = getString(fld.name))
        is DataType.nullableText ->
          Value.nullableText(value = getObject(fld.name) as? String?)
      }
  }
