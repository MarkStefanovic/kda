package kda.adapter.pg

import kda.adapter.std.StdSQLAdapter
import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.SQLAdapter

class PgSQLAdapter(private val stdImpl: SQLAdapter) : SQLAdapter by stdImpl

val pgSQLAdapter =
  PgSQLAdapter(StdSQLAdapter(PgSQLAdapterImplDetails(StdSQLAdapterImplDetails())))
