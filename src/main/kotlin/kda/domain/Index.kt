package kda.domain

data class Index(val tableName: String, val fields: List<Pair<String, Boolean>>) {
  val name: String by lazy {
    "ix_${tableName}_" + fields.joinToString("_") { it.first }
  }
}
