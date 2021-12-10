package kda.domain

data class Table(
  val name: String,
  val fields: Set<Field<*>>,
  val primaryKeyFieldNames: List<String>,
) {
  val primaryKeyFields: List<Field<*>> by lazy {
    val fieldLookup: Map<String, Field<*>> = fields.associateBy { it.name }
    primaryKeyFieldNames.map { fieldName ->
      fieldLookup[fieldName]
        ?: throw KDAError.FieldNotFound(
          fieldName = fieldName,
          availableFieldNames = fields.map { it.name }.toSet(),
        )
    }
  }

  fun field(name: String): Field<*> =
    fields.firstOrNull { it.name == name }
      ?: throw KDAError.FieldNotFound(fieldName = name, availableFieldNames = fields.map { it.name }.toSet())

  override fun toString(): String =
    """
    |Table [
    |  name: $name
    |  fields: ${fields.sortedBy { it.name }.joinToString(", ")}
    |  primaryKeyFieldNames: ${primaryKeyFieldNames.joinToString(", ")}
    |]
    """.trimMargin()
}
