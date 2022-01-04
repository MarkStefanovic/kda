@file:Suppress("SqlDialectInspection")

package kda.adapter.std

import kda.adapter.applyBoundParameters
import kda.adapter.applyRow
import kda.adapter.getValue
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
import kda.domain.DbAdapterDetails
import kda.domain.Field
import kda.domain.KDAError
import kda.domain.OrderBy
import kda.domain.Parameter
import kda.domain.Row
import kda.domain.Table
import java.sql.Connection

@ExperimentalStdlibApi
class StdAdapter(
  private val con: Connection,
  private val showSQL: Boolean,
  private val details: DbAdapterDetails,
) : Adapter {

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
      println(sql)
    }

    con.createStatement().use { statement ->
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
        println(sql)
      }

      con.prepareStatement(sql).use { statement ->
        statement.applyBoundParameters(criteria.boundParameters)

        statement.executeUpdate()
      }
    }

  override fun deleteAll(schema: String?, table: String): Int {
    val fullTableName = details.fullTableName(schema = schema, table = table)

    val sql = "TRUNCATE $fullTableName"

    if (showSQL) {
      println(sql)
    }

    return con.createStatement().use { statement ->
      statement.executeUpdate(sql)
    }
  }

  override fun deleteRows(schema: String?, table: String, fields: Set<Field<*>>, keys: Set<Row>): Int {
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
        |StdAdapter.deleteRows:
        |  SQL: $sql
        |  Parameters: 
        |    $whereClauseParameters
      """.trimMargin()
      )
    }

    return con.prepareStatement(sql).use { preparedStatement ->
      keys.forEach { row ->
        preparedStatement.applyRow(row = row, parameters = whereClauseParameters)
        preparedStatement.addBatch()
      }
      preparedStatement.executeBatch().toList().sum()
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
          |StdAdapter.select - select rows when criteria is not null:
          |  SQL:
          |    ${sql.split("\n").joinToString("\n    ")}
          |  Parameters: 
          |    ${criteria.boundParameters.joinToString("\n    ")}
        """.trimMargin()
        )
      }

      sequence<Row> {
        con.prepareStatement(sql).use { preparedStatement ->
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
              |StdAdapter.select:
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
        if (showSQL) {
          println(baseSQL)
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
        |StdAdapter.selectGreatest:
        |  ${sql.split("\n").joinToString("\n  ")}
      """.trimMargin()
      )
    }

    con.createStatement().use { statement ->
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
              |StdAdapter.selectRows: select rows from $fullTableName matching row:
              |  SQL:
              |    ${sql.split("\n").joinToString("\n    ")}
              |  Parameters:
              |    $paramsStr
            """.trimMargin()
            )
          }

          con.prepareStatement(sql).use { statement ->
            statement.applyBoundParameters(parameters = parameters.flatten())

            statement.executeQuery().use { rs ->
              yieldAll(rs.toRows(fields))
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
        it.name
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
          |StdAdapter.upsertRows - insert rows:
          |  SQL:
          |    ${sql.split("\n").joinToString("\n    ")}
          |  Parameters:
          |    ${parameters.joinToString("\n    ") { it.toString() }}
        """.trimMargin()
        )
      }

      con.prepareStatement(sql).use { statement ->
        rows.forEach { row ->
          statement.applyRow(parameters = parameters, row = row)

          statement.addBatch()
        }
        statement.executeBatch().asList().sum()
      }
    }
}
