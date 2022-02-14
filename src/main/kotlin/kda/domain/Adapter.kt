package kda.domain

@ExperimentalStdlibApi
interface Adapter {
  fun addRows(
    schema: String?,
    table: String,
    rows: Iterable<Row>,
    fields: Set<Field<*>>,
  ): Int

  fun createTable(
    schema: String?,
    table: Table,
  )

  fun delete(
    schema: String?,
    table: String,
    criteria: Criteria,
  ): Int

  fun deleteAll(
    schema: String?,
    table: String,
  ): Int

  fun deleteRows(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    keys: Set<Row>,
  ): Int

  fun rowCount(
    schema: String?,
    table: String,
  ): Int

  fun select(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    criteria: Criteria?,
    batchSize: Int,
    limit: Int?,
    orderBy: List<OrderBy>,
  ): Sequence<Row>

  fun selectAll(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    batchSize: Int,
    orderBy: List<OrderBy>,
  ): Sequence<Row>

  fun selectGreatest(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
  ): Any?

  fun selectRows(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    keys: Set<Row>,
    batchSize: Int,
    orderBy: List<OrderBy>,
  ): Sequence<Row>

  fun upsertRows(
    schema: String?,
    table: String,
    rows: Set<Row>,
    keyFields: Set<Field<*>>,
    valueFields: Set<Field<*>>,
  ): Int
}
