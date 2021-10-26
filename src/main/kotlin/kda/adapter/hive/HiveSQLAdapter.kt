package kda.adapter.hive

import kda.adapter.std.StdSQLAdapter
import kda.domain.SQLAdapterImplDetails

class HiveSQLAdapter(impl: SQLAdapterImplDetails) : StdSQLAdapter(impl)

val hiveSQLAdapter by lazy { HiveSQLAdapter(HiveSQLAdapterImplDetails()) }
