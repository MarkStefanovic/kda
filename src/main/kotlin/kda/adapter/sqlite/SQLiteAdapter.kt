package kda.adapter.sqlite

import kda.adapter.std.StdSQLAdapter
import kda.domain.SQLAdapterImplDetails

class SQLiteAdapter(private val impl: SQLAdapterImplDetails) : StdSQLAdapter(impl)

val sqliteAdapter by lazy { SQLiteAdapter(SQLiteAdapterImplDetails()) }
