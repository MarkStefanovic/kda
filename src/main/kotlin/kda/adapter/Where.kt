package kda.adapter

import kda.domain.Criteria
import kda.domain.DbDialect

@ExperimentalStdlibApi
fun where(dialect: DbDialect): Criteria = Criteria.empty(dialect)
