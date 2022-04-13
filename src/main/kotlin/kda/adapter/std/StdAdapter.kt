@file:Suppress("SqlDialectInspection")

package kda.adapter.std

import kda.adapter.applyBoundParameters
import kda.adapter.applyRow
import kda.adapter.toMap
import kda.adapter.toRows
import kda.adapter.toSQL
import kda.adapter.toSetValuesEqualToParameters
import kda.adapter.toValuesParameter
import kda.adapter.toWhereEqualsBoundParameters
import kda.adapter.toWhereEqualsParameter
import kda.domain.Adapter
import kda.domain.BoundParameter
import kda.domain.Criteria
import kda.domain.DataType
import kda.domain.DbAdapterDetails
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.OrderBy
import kda.domain.Parameter
import kda.domain.Row
import kda.domain.Table
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ExperimentalStdlibApi
class StdAdapter(
  private val con: Connection,
  private val showSQL: Boolean,
  private val details: DbAdapterDetails,
  private val queryTimeout: Duration,
) : Adapter {
  override fun addRows(
    schema: String?,
    table: String,
    rows: Iterable<Row>,
    fields: Set<Field<*>>,
  ): Int {
    val newRows = rows.toList()

    return if (newRows.isEmpty()) {
      0
    } else {
      val fieldNameCSV = newRows.first().fieldsSorted.joinToString(", ") { details.wrapName(it) }
      val placeholderCSV = newRows.first().fields.joinToString(", ") { "?" }
      val fullTableName = details.fullTableName(schema = schema, table = table)

      val sql = """
        |INSERT INTO $fullTableName ($fieldNameCSV)
        |VALUES ($placeholderCSV)
      """.trimMargin()

      if (showSQL) {
        println(
          """
          |StdAdapter.upsertRows(schema = $schema, table = $table, rows = $rows, fields = $fields):
          |  SQL:
          |    ${sql.split("\n").joinToString("\n    ")}
          """.trimMargin()
        )
      }

      val parameters = fields.sortedBy { it.name }.map { it.toValuesParameter() }

      con.prepareStatement(sql).use { statement ->
        statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

        newRows.forEach { row ->
          statement.applyRow(parameters = parameters, row = row)

          statement.addBatch()
        }
        statement.executeBatch().asList().sum()
      }
    }
  }

  override fun createTable(schema: String?, table: Table) {
    val fieldDefCSV: String =
      table
        .fields
        .sortedBy { it.name }
        .joinToString(",  ") { details.fieldDef(it) }

    val pkFieldCSV = table.primaryKeyFieldNames.joinToString(", ") { details.wrapName(it) }

    val fullTableName = details.fullTableName(schema = schema, table = table.name)

    val sql: String =
      """
        |CREATE TABLE IF NOT EXISTS $fullTableName (
        |  $fieldDefCSV
        |, PRIMARY KEY ($pkFieldCSV)
        |)
      """.trimMargin()

    if (showSQL) {
      println(
        """
          |StdAdapter.createTable(schema = $schema, table = $table):
          |  ${sql.split("\n").joinToString("\n  ")}
          """.trimMargin()
      )
    }

    con.createStatement().use { statement ->
      statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()
      statement.execute(sql)
    }
  }

  override fun delete(
    schema: String?,
    table: String,
    criteria: Criteria,
  ): Int =
    if (criteria.boundParameters.isEmpty()) {
      0
    } else {
      val fullTableName = details.fullTableName(schema = schema, table = table)

      val sql = "DELETE FROM $fullTableName WHERE ${criteria.sql}"

      if (showSQL) {
        println(
          """
          |StdAdapter.delete(schema = $schema, table = $table, criteria = $criteria):
          |  ${sql.split("\n").joinToString("\n  ")}
          """.trimMargin()
        )
      }

      con.prepareStatement(sql).use { statement ->
        statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

        statement.applyBoundParameters(criteria.boundParameters)

        statement.executeUpdate()
      }
    }

  override fun deleteAll(schema: String?, table: String): Int {
    val fullTableName = details.fullTableName(schema = schema, table = table)

    val sql = "TRUNCATE $fullTableName"

    if (showSQL) {
      println(
        """
        |StdAdapter.deleteAll(schema = $schema, table = $table):
        |  ${sql.split("\n").joinToString("\n  ")}
        """.trimMargin()
      )
    }

    return con.createStatement().use { statement ->
      statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

      statement.executeUpdate(sql)
    }
  }

  override fun deleteRows(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    keys: Set<Row>,
  ): Int {
    val fullTableName = details.fullTableName(schema = schema, table = table)

    val fieldLookup = fields.associateBy { it.name }

    val firstRow: Row = keys.first()

    val keyFields: List<Field<*>> =
      firstRow
        .value
        .keys
        .map { fieldName ->
          fieldLookup[fieldName]
            ?: throw KDAError.FieldNotFound(fieldName = fieldName, availableFieldNames = fields.map { it.name }.toSet())
        }
        .sortedBy { it.name }

    val whereClauseParameters: List<Parameter> = keyFields.map { field ->
      field.toWhereEqualsParameter(details = details)
    }

    val whereClauseSQL = whereClauseParameters.joinToString(" AND ") { it.sql }

    val sql = "DELETE FROM $fullTableName WHERE $whereClauseSQL"

    if (showSQL) {
      println(
        """
        |StdAdapter.deleteRows(schema = $schema, table = $table, fields = $fields, keys = $keys):
        |  SQL: $sql
        |  Parameters: 
        |    $whereClauseParameters
      """.trimMargin()
      )
    }

    return con.prepareStatement(sql).use { preparedStatement ->
      keys.forEach { row ->
        preparedStatement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

        preparedStatement.applyRow(row = row, parameters = whereClauseParameters)
        preparedStatement.addBatch()
      }
      preparedStatement.executeBatch().toList().sum()
    }
  }

  override fun rowCount(schema: String?, table: String): Int {
    val fullTableName = details.fullTableName(schema = schema, table = table)

    val sql = """
      |SELECT COUNT(*) AS ct 
      |FROM $fullTableName
    """.trimMargin()

    if (showSQL) {
      println(
        """
          |StdAdapter.rowCount(schema = $schema, table = $table):
          |  ${sql.split("\n").joinToString("\n    ")}
        """.trimMargin()
      )
    }

    con.createStatement().use { statement ->
      statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

      statement.executeQuery(sql).use { resultSet ->
        resultSet.next()
        return resultSet.getInt(1)
      }
    }
  }

  override fun select(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    criteria: Criteria?,
    batchSize: Int,
    limit: Int?,
    orderBy: List<OrderBy>,
  ): Sequence<Row> =
    if (criteria == null) {
      selectAll(
        schema = schema,
        table = table,
        fields = fields,
        batchSize = batchSize,
        orderBy = orderBy,
      )
    } else {
      val fullTableName = details.fullTableName(schema = schema, table = table)

      val fieldNameCSV: String =
        fields
          .sortedBy { it.name }
          .joinToString(", ") { details.wrapName(it.name) }

      val orderByClause = if (orderBy.isEmpty()) {
        ""
      } else {
        "ORDER BY ${orderBy.toSQL(details = details)}"
      }

      val sql = """
        |SELECT $fieldNameCSV 
        |FROM $fullTableName 
        |WHERE ${criteria.sql}
        |$orderByClause
      """.trimMargin()

      if (showSQL) {
        println(
          """
          |StdAdapter.select(schema = $schema, table = $table, fields = $fields, criteria = $criteria, batchSize = $batchSize, limit = $limit, orderBy = $orderBy):
          |  SQL:
          |    ${sql.split("\n").joinToString("\n    ")}
          |  Parameters: 
          |    ${criteria.boundParameters.joinToString("\n    ")}
        """.trimMargin()
        )
      }

      sequence<Row> {
        con.prepareStatement(sql).use { preparedStatement ->
          preparedStatement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

          preparedStatement.applyBoundParameters(criteria.boundParameters)
          try {
            preparedStatement.executeQuery().use { rs ->
              while (rs.next()) {
                try {
                  yield(rs.toMap(fields))
                } catch (e: Throwable) {
                  println("StdAdapter.select(...) - yield(rs.toMap($fields)):")
                  println("  criteria.boundParameters: ${criteria.boundParameters}")
                  e.printStackTrace()
                }
              }
            }
          } catch (e: Throwable) {
            println(
              """
              |StdAdapter.select(schema = $schema, table = $table, fields = $fields, criteria = $criteria, batchSize = $batchSize, limit = $limit, orderBy = $orderBy):
              |  preparedStatement:
              |    ${preparedStatement.toString().split("\n").joinToString("\n    ")}
              |  criteria: 
              |    $criteria
              """.trimMargin()
            )
            throw e
          }
        }
      }
    }

  override fun selectAll(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    batchSize: Int,
    orderBy: List<OrderBy>,
  ): Sequence<Row> {
    val fullTableName = details.fullTableName(schema = schema, table = table)

    val fieldNameCSV: String =
      fields
        .sortedBy { it.name }
        .joinToString(", ") { details.wrapName(it.name) }

    val orderByClause = if (orderBy.isEmpty()) {
      ""
    } else {
      " " + orderBy.toSQL(details = details)
    }

    val baseSQL = "SELECT $fieldNameCSV FROM $fullTableName$orderByClause"

    return sequence {
      con.createStatement().use { statement ->
        statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

        if (showSQL) {
          println(
            """
              |StdAdapter.selectAll(schema = $schema, table = $table, fields = $fields, batchSize = $batchSize, orderBy = $orderBy):
              |  ${baseSQL.split("\n").joinToString("\n  ")}
            """.trimMargin()
          )
        }

        statement.executeQuery(baseSQL).use { rs ->
          while (rs.next()) {
            yield(rs.toMap(fields))
          }
        }
      }
    }
  }

  override fun selectGreatest(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
  ): Any? {
    val fullTableName = details.fullTableName(schema = schema, table = table)

    val fieldNameCSV: String =
      fields
        .sortedBy { it.name }
        .joinToString(", ") { details.wrapName(it.name) }

    val sql = """
      |SELECT GREATEST($fieldNameCSV) AS greatest_val
      |FROM $fullTableName
    """.trimMargin()

    if (showSQL) {
      println(
        """
        |StdAdapter.selectGreatest(schema = $schema, table = $table, fields = $fields):
        |  ${sql.split("\n").joinToString("\n  ")}
      """.trimMargin()
      )
    }

    con.createStatement().use { statement ->
      statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

      statement.executeQuery(sql).use { rs ->
        return if (rs.next()) {
          rs.getObject("greatest_val")
        } else {
          null
        }
      }
    }
  }

  override fun selectRows(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    keys: Set<Row>,
    batchSize: Int,
    orderBy: List<OrderBy>,
  ): Sequence<Row> {
    val fullTableName = details.fullTableName(
      schema = schema,
      table = table,
    )

    val fieldNameCSV: String =
      fields
        .sortedBy { it.name }
        .joinToString(", ") { details.wrapName(it.name) }

    val orderByClause = if (orderBy.isEmpty()) {
      ""
    } else {
      " " + orderBy.toSQL(details)
    }

    val baseSQL = """
      |SELECT $fieldNameCSV
      |FROM $fullTableName
    """.trimMargin()

    return if (keys.isEmpty()) {
      sequence {
        con.createStatement().use { statement ->
          statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

          if (showSQL) {
            println(baseSQL)
          }

          val sql = """
            |$baseSQL
            |$orderByClause
          """.trimMargin()

          statement.executeQuery(sql).use { rs ->
            while (rs.next()) {
              yield(rs.toMap(fields))
            }
          }
        }
      }
    } else {
      if (keys.first().fields.count() == 1) {
        val fieldName = keys.first().fields.first()

        val field = fields.first { field -> field.name == fieldName }

        assert(!fields.first { it.name == fieldName }.dataType.nullable) {
          "Key fields must not be nullable."
        }

        sequence {
          keys.chunked(batchSize).forEach { rows ->
            val wrappedFieldName = details.wrapName(name = fieldName)
            val inClause = rows.joinToString(",") { "?" }
            val whereClause = "WHERE $wrappedFieldName IN ($inClause)"
            val sql = "$baseSQL $whereClause"

            if (showSQL) {
              println(
                """
                |StdAdapter.selectRows(schema = $schema, table = $table, fields = $fields, keys = $keys, batchSize = $batchSize, orderBy = $orderBy):
                |  SQL:
                |    ${sql.split("\n").joinToString("\n    ")}
                |  Parameters:
                |    ${rows.take(5)}...
              """.trimMargin()
              )
            }

            con.prepareStatement(sql).use { statement ->
              statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

              rows.forEachIndexed { index, row ->
                when (field.dataType) {
                  DataType.bigInt -> statement.setLong(index + 1, (row.value[fieldName] as Number).toLong())
                  DataType.bool -> statement.setBoolean(index + 1, row.value[fieldName] as Boolean)
                  is DataType.decimal -> statement.setBigDecimal(index + 1, row.value[fieldName] as BigDecimal)
                  DataType.float -> statement.setFloat(index + 1, row.value[fieldName] as Float)
                  DataType.int -> statement.setInt(index + 1, (row.value[fieldName] as Number).toInt())
                  DataType.localDate -> statement.setDate(index + 1, Date.valueOf(row.value[fieldName] as LocalDate))
                  DataType.localDateTime -> statement.setTimestamp(index + 1, Timestamp.valueOf(row.value[fieldName] as LocalDateTime))
                  is DataType.text -> statement.setString(index + 1, row.value[fieldName] as String)
                  else -> error("Key field cannot be nullable, but $fieldName is of type ${field.dataType}.")
                }
              }

              statement.executeQuery().use { rs ->
                yieldAll(rs.toRows(fields))
              }
            }
          }
        }
      } else {
        sequence {
          val fieldLookup = fields.associateBy { it.name }
          keys.chunked(batchSize).forEach { rows ->
            val parameters: List<List<BoundParameter>> = rows.map { row ->
              val sortedFields = row.fieldsSorted.map { fieldName ->
                fieldLookup[fieldName]
                  ?: throw KDAError.FieldNotFound(
                    fieldName = fieldName,
                    availableFieldNames = fields.map { it.name }.toSet(),
                  )
              }
              sortedFields.toWhereEqualsBoundParameters(details = details, row = row)
            }

            val whereClause = parameters.joinToString(" OR ") { orGroup ->
              val sql = orGroup.joinToString(" OR ") { boundParameter ->
                "(${boundParameter.parameter.sql})"
              }
              "($sql)"
            }
            val sql = """
              |$baseSQL 
              |WHERE $whereClause
              |$orderByClause
            """.trimMargin()

            if (showSQL) {
              val paramsStr =
                parameters
                  .mapIndexed { ix, params ->
                    val ps = params.mapIndexed { paramIndex, param ->
                      "  (${paramIndex + 1})  $param"
                    }.joinToString("\n    ")
                    "(${ix + 1})\n    $ps"
                  }
                  .joinToString("\n    ")
              println(
                """
                |StdAdapter.selectRows(schema = $schema, table = $table, fields = $fields, keys = $keys, batchSize = $batchSize, orderBy = $orderBy):
                |  SQL:
                |    ${sql.split("\n").joinToString("\n    ")}
                |  Parameters:
                |    $paramsStr
              """.trimMargin()
              )
            }

            con.prepareStatement(sql).use { statement ->
              statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

              statement.applyBoundParameters(parameters = parameters.flatten())

              statement.executeQuery().use { rs ->
                yieldAll(rs.toRows(fields))
              }
            }
          }
        }
      }
    }
  }

  override fun upsertRows(
    schema: String?,
    table: String,
    rows: Set<Row>,
    keyFields: Set<Field<*>>,
    valueFields: Set<Field<*>>,
  ): Int =
    if (rows.isEmpty()) {
      0
    } else {
      val sortedKeyFields = keyFields.sortedBy { it.name }

      val allFields = (keyFields + valueFields).sortedBy { it.name }

      val fullTableName = details.fullTableName(schema = schema, table = table)

      val allFieldCSV = allFields.joinToString(", ") { details.wrapName(it.name) }

      val keyFieldCSV = sortedKeyFields.joinToString(", ") {
        details.wrapName(it.name)
      }

      val valueClauseParameters: List<Parameter> =
        allFields
          .map { field ->
            field.toValuesParameter()
          }

      val setClauseParameters: List<Parameter> =
        valueFields
          .sortedBy { it.name }
          .toSetValuesEqualToParameters(details = details)

      val parameters = valueClauseParameters + setClauseParameters

      val valuesSQL = valueClauseParameters.joinToString(", ") {
        it.sql
      }

      val setClauseSQL = setClauseParameters.joinToString(", ") {
        it.sql
      }

      val fullyQualifiedSrcFieldNameCSV: String = valueFields.sortedBy { it.name }.joinToString(", ") { field ->
        val wrapppedFieldName = details.wrapName(field.name)
        "$fullTableName.$wrapppedFieldName"
      }

      val fullyQualifiedDstFieldNameCSV: String = valueFields.sortedBy { it.name }.joinToString(", ") { field ->
        val wrapppedFieldName = details.wrapName(field.name)
        "EXCLUDED.$wrapppedFieldName"
      }

      val whereClause = """
        |WHERE (
        |  $fullyQualifiedSrcFieldNameCSV
        |) IS DISTINCT FROM (
        |  $fullyQualifiedDstFieldNameCSV
        |)
      """.trimMargin()

      val sql = """
        |INSERT INTO $fullTableName ($allFieldCSV)
        |VALUES ($valuesSQL)
        |ON CONFLICT ($keyFieldCSV)
        |DO UPDATE SET $setClauseSQL
        |$whereClause
      """.trimMargin()

      if (showSQL) {
        println(
          """
          |StdAdapter.upsertRows(schema = $schema, table = $table, rows = $rows, keyFields = $keyFields, valueFields = $valueFields):
          |  SQL:
          |    ${sql.split("\n").joinToString("\n    ")}
          |  Parameters:
          |    ${parameters.joinToString("\n    ") { it.toString() }}
        """.trimMargin()
        )
      }

      con.prepareStatement(sql).use { statement ->
        statement.queryTimeout = queryTimeout.inWholeSeconds.toInt()

        rows.forEach { row ->
          statement.applyRow(parameters = parameters, row = row)

          statement.addBatch()
        }
        statement.executeBatch().asList().sum()
      }
    }
}

@ExperimentalStdlibApi
fun renderInClause(wrappedName: String, criteria: Set<Criteria>): String {
  require(
    criteria.all { c ->
      c.boundParameters.count() == 1 && c.boundParameters.all { boundParameter ->
        !boundParameter.parameter.dataType.nullable
      }
    } && criteria.flatMap { c ->
      c.boundParameters.map { boundParameter ->
        boundParameter.parameter.name
      }
    }.toSet().count() == 1
  ) {
    "Can only use an IN clause when all criteria are based on a single, non-nullable field."
  }
  val valuePlaceholders = criteria.joinToString(",") { _ -> "?" }
  return "$wrappedName IN ($valuePlaceholders)"
}
