package kda.adapter.mssql

import kda.adapter.std.StdSQLAdapter
import kda.domain.SQLAdapterImplDetails

class MSSQLAdapter(private val impl: SQLAdapterImplDetails) : StdSQLAdapter(impl)

val msSQLAdapter = MSSQLAdapter(MSSQLAdapterImplDetails())
