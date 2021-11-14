package kda.adapter

import kda.domain.Criteria
import kda.domain.Datasource
import kda.domain.LatestTimestamp
import kda.domain.LatestTimestampRepository
import kda.domain.Operator
import kda.domain.Predicate
import java.time.LocalDateTime

class DbLatestTimestampRepository(
  private val ds: Datasource,
  val showSQL: Boolean,
) : LatestTimestampRepository {

  override fun add(
    schema: String?,
    table: String,
    latestTimestamp: LatestTimestamp,
  ) {
    val sql = ds.adapter.merge(
      table = latestTimestamps,
      rows = setOf(
        latestTimestamps.row(
          "schema_name" to schema,
          "table_name" to table,
          "field_name" to latestTimestamp.fieldName,
          "ts" to latestTimestamp.timestamp,
        )
      )
    )

    if (showSQL) {
      println(sql)
    }

    ds.executor.execute(sql)
  }

  override fun delete(schema: String?, table: String) {
    val sql = ds.adapter.delete(
      table = latestTimestamps,
      rows = setOf(
        latestTimestamps.row(
          "schema_name" to schema,
          "table_name" to table,
        ),
      )
    )

    if (showSQL) {
      println(sql)
    }

    ds.executor.execute(sql)
  }

  override fun get(schema: String?, table: String): Set<LatestTimestamp> {
    val schemaField = latestTimestamps.field("schema_name")

    val tableField = latestTimestamps.field("table_name")

    val criteria = Criteria(
      setOf(
        setOf(
          Predicate(
            field = schemaField,
            operator = Operator.Equals,
            value = schemaField.wrapValue(schema),
          ),
          Predicate(
            field = tableField,
            operator = Operator.Equals,
            value = tableField.wrapValue(table),
          ),
        ),
      ),
    )

    val sql = ds.adapter.select(table = latestTimestamps, criteria = criteria)

    if (showSQL) {
      println(sql)
    }

    return ds.executor
      .fetchRows(sql = sql, fields = latestTimestamps.fields)
      .map { row ->
        LatestTimestamp(
          fieldName = row.value("field_name").value as String,
          timestamp = row.value("ts").value as? LocalDateTime,
        )
      }
      .toSet()
  }
}
