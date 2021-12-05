package kda.domain

@ExperimentalStdlibApi
interface Adapter {

  fun createTable(
    schema: String?,
    table: Table,
  )

  fun delete(
    schema: String?,
    table: String,
    criteria: Criteria,
  )

  fun deleteAll(
    schema: String?,
    table: String,
  )

  fun deleteRows(
    schema: String?,
    table: String,
    fields: Set<Field<*>>,
    keys: Set<Row>,
  )

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
  )
}