@file:Suppress("DuplicatedCode")

package kda.domain

import kda.adapter.hh.HHAdapterDetails
import kda.adapter.mssql.MSSQLAdapterDetails
import kda.adapter.pg.PgAdapterDetails
import kda.adapter.sqlite.SQLiteAdapterDetails

@ExperimentalStdlibApi
data class Criteria(
  private val dialect: DbDialect,
  val sql: String? = null,
  val boundParameters: List<BoundParameter> = emptyList(),
) {
  init {
    require((sql == null) || sql.isNotEmpty()) {
      "If sql is not null, then it must be at least 1 character."
    }

    require(!((sql == null) && boundParameters.isNotEmpty())) {
      "If sql is null, then boundBarameters should be empty, but boundParameters are as follows: $boundParameters."
    }
  }

  val isEmpty: Boolean
    get() = boundParameters.isEmpty()

  private val details: DbAdapterDetails by lazy {
    when (dialect) {
      DbDialect.HH -> HHAdapterDetails
      DbDialect.MSSQL -> MSSQLAdapterDetails
      DbDialect.PostgreSQL -> PgAdapterDetails
      DbDialect.SQLite -> SQLiteAdapterDetails
    }
  }

  fun and(binaryPredicate: BinaryPredicate<*>): Criteria {
    val newParams: List<BoundParameter> = binaryPredicate.toBoundParameters(details = details).sortedBy { it.parameter.name }

    val paramSQL = newParams.joinToString(" OR ") { boundParameter -> boundParameter.parameter.sql }

    return if (isEmpty) {
      Criteria(
        dialect = dialect,
        sql = paramSQL,
        boundParameters = newParams,
      )
    } else {
      Criteria(
        dialect = dialect,
        sql = "($sql) AND ($paramSQL)",
        boundParameters = boundParameters + newParams,
      )
    }
  }

  fun or(binaryPredicate: BinaryPredicate<*>): Criteria {
    val newParams: List<BoundParameter> = binaryPredicate.toBoundParameters(details = details).sortedBy { it.parameter.name }

    val paramSQL = newParams.joinToString(" OR ") { boundParameter -> boundParameter.parameter.sql }

    return if (isEmpty) {
      Criteria(
        dialect = dialect,
        sql = paramSQL,
        boundParameters = newParams,
      )
    } else {
      Criteria(
        dialect = dialect,
        sql = "($sql) OR ($paramSQL)",
        boundParameters = boundParameters + newParams,
      )
    }
  }

  fun and(other: Criteria): Criteria =
    if (isEmpty) {
      Criteria(
        dialect = dialect,
        sql = other.sql,
        boundParameters = other.boundParameters,
      )
    } else {
      Criteria(
        dialect = dialect,
        sql = "($sql) AND (${other.sql})",
        boundParameters = boundParameters + other.boundParameters,
      )
    }

  fun or(other: Criteria): Criteria =
    if (isEmpty) {
      Criteria(
        dialect = dialect,
        sql = other.sql,
        boundParameters = other.boundParameters,
      )
    } else {
      Criteria(
        dialect = dialect,
        sql = "($sql) OR (${other.sql})",
        boundParameters = boundParameters + other.boundParameters,
      )
    }

  override fun toString(): String =
    """
      |Criteria [
      |  dialect: $dialect
      |  sql: $sql
      |  boundParameters: $boundParameters
      |]
    """.trimMargin()

  companion object {
    fun empty(dialect: DbDialect): Criteria =
      Criteria(dialect = dialect, sql = null, boundParameters = emptyList())
  }
}
