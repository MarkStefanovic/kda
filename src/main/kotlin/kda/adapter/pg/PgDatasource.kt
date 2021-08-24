package kda.adapter.pg

import kda.adapter.JdbcExecutor
import kda.domain.Datasource
import java.sql.Connection

fun pgDatasource(con: Connection): Datasource {
    val executor = JdbcExecutor(con = con)
    return Datasource(
        connection = con,
        adapter = pgSQLAdapter,
        executor = executor,
        inspector = PgInspector(sqlExecutor = executor),
    )
}
