package kda.domain

data class Table(
  val schema: String?,
  val name: String,
  val fields: Set<Field>,
  val primaryKeyFieldNames: List<String>,
) {
  init {
    if (schema != null)
      require(schema.isNotBlank()) { "If a schema is provided, it must be at least 1 char." }

    require(name.isNotBlank()) { "A table's name must be at least 1 char, but got $name." }

    require(primaryKeyFieldNames.isNotEmpty()) {
      "A table must have primary key fields, but no primary key fields were provided."
    }

    require(fields.isNotEmpty()) { "A table must have fields." }
  }

  val sortedFieldNames: List<String> by lazy { fields.map { fld -> fld.name }.sorted() }

  fun row(vararg keyValuePairs: Pair<String, Any?>) = Row(
    keyValuePairs.associate { (fieldName, value) ->
      val field = fields.find { field -> field.name == fieldName }
      val wrappedValue = field?.dataType?.wrapValue(value) ?: throw KDAError.FieldNotFound(
        fieldName = fieldName,
        availableFieldNames = fields.map { it.name }.toSet(),
      )
      fieldName to wrappedValue
    }
  )

  fun rows(vararg rowMaps: Map<String, Any?>): List<Row> =
    rowMaps.map { map -> row(*map.entries.map { it.toPair() }.toTypedArray()) }

  fun subset(fieldNames: Set<String>): Table =
    copy(fields = fields.filter { fld -> fld.name in fieldNames }.toSet())
}
