package kda.adapter

import kda.domain.DataType
import kda.domain.Datasource
import kda.domain.Field
import kda.domain.Table

fun createTables(ds: Datasource, showSQL: Boolean) {
  val tables = setOf(tableDefs, primaryKeys, latestTimestamps)

  tables.forEach { table ->
    val tableExistsSQL = ds.inspector.tableExists(schema = null, table = table.name)

    if (!tableExistsSQL) {
      val sql = ds.adapter.createTable(table)

      if (showSQL) {
        println(sql)
      }

      ds.executor.execute(sql)
    }
  }
}

val tableDefs = Table(
  schema = null,
  name = "kda_table_def",
  fields = setOf(
    Field(name = "schema_name", dataType = DataType.nullableText(null)),
    Field(name = "table_name", dataType = DataType.text(null)),
    Field(name = "field_name", dataType = DataType.text(null)),
    Field(name = "data_type", dataType = DataType.text(40)),
    Field(name = "max_length", dataType = DataType.nullableInt(false)),
    Field(name = "precision", dataType = DataType.nullableInt(false)),
    Field(name = "scale", dataType = DataType.nullableInt(false)),
    Field(name = "autoincrement", dataType = DataType.nullableBool),
    Field(name = "date_added", dataType = DataType.localDateTime),
  ),
  primaryKeyFieldNames = listOf("schema_name", "table_name", "field_name"),
)

val primaryKeys = Table(
  schema = null,
  name = "kda_primary_key",
  fields = setOf(
    Field(name = "schema_name", dataType = DataType.nullableText(null)),
    Field(name = "table_name", dataType = DataType.text(null)),
    Field(name = "field_name", dataType = DataType.text(null)),
    Field(name = "order", dataType = DataType.int(false)),
    Field(name = "date_added", dataType = DataType.localDateTime),
  ),
  primaryKeyFieldNames = listOf("schema_name", "table_name", "order"),
)

val latestTimestamps = Table(
  schema = null,
  name = "kda_latest_timestamp",
  fields = setOf(
    Field(name = "schema_name", dataType = DataType.nullableText(null)),
    Field(name = "table_name", dataType = DataType.text(null)),
    Field(name = "field_name", dataType = DataType.text(null)),
    Field(name = "ts", dataType = DataType.nullableLocalDateTime),
  ),
  primaryKeyFieldNames = listOf("schema_name", "table_name", "field_name"),
)
