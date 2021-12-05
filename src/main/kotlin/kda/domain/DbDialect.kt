package kda.domain

enum class DbDialect {
  HH,
  MSSQL,
  PostgreSQL,
  SQLite;

  override fun toString(): String =
    when (this) {
      HH         -> "HH"
      MSSQL      -> "MSSQL"
      PostgreSQL -> "PostgreSQL"
      SQLite     -> "SQLite"
    }
}
