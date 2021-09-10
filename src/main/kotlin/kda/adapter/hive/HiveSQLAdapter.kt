package kda.adapter.hive

import kda.adapter.std.StdSQLAdapter
import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.SQLAdapter

class HiveSQLAdapter(private val std: SQLAdapter) : SQLAdapter by std

private val implDetails = HiveSQLAdapterImplDetails(StdSQLAdapterImplDetails())

val hiveSQLAdapter = HiveSQLAdapter(std = StdSQLAdapter(implDetails))
