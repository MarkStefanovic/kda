package kda.adapter.hive

import kda.adapter.std.StdSQLAdapter
import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.SQLAdapter

class HiveSQLAdapter(private val stdImpl: SQLAdapter) : SQLAdapter by stdImpl

val hiveSQLAdapter = HiveSQLAdapter(StdSQLAdapter(HiveSQLAdapterImplDetails(StdSQLAdapterImplDetails())))
