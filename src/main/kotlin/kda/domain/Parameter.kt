package kda.domain

data class Parameter(
  val name: String,
  val dataType: DataType<*>,
  val sql: String,
) {
  init {
    require(sql.isNotEmpty()) {
      "The $name parameter is blank."
    }
  }
}
