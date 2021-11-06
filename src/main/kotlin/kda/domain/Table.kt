package kda.domain

import kotlin.collections.joinToString

data class Table(
  val schema: String?,
  val name: String,
  val fields: Set<Field>,
  val primaryKeyFieldNames: List<String>,
) {
  init {
    if (schema != null) {
      require(schema.isNotBlank()) { "If a schema is provided, it must be at least 1 char." }
    }

    require(name.isNotBlank()) { "A table's name must be at least 1 char, but got $name." }

    require(primaryKeyFieldNames.isNotEmpty()) {
      "A table must have primary key fields, but no primary key fields were provided."
    }

    require(fields.isNotEmpty()) { "A table must have fields." }
  }

  val sortedFieldNames: List<String> by lazy { fields.map { fld -> fld.name }.sorted() }

  val primaryKeyFields: List<Field> by lazy {
    val fldLkp = fields.associateBy { it.name }
    primaryKeyFieldNames.map {
      fldLkp[it] ?: throw KDAError.FieldNotFound(fieldName = it, availableFieldNames = fldLkp.keys)
    }
  }

  fun row(vararg keyValuePairs: Pair<String, Any?>): Row {
    val rowMap =
      keyValuePairs.associate { (fieldName, value) ->
        wrapFieldValue(fields = fields, fieldName = fieldName, value = value)
      }
    return Row(rowMap)
  }

  fun rows(vararg rowMaps: Map<String, Any?>): Set<Row> =
    rowMaps.map { map -> row(*map.entries.map { it.toPair() }.toTypedArray()) }.toSet()

  fun subset(fieldNames: Set<String>): Table =
    copy(fields = fields.filter { fld -> fld.name in fieldNames }.toSet())

  override fun toString() =
    """
    |Table [
    |  schema: $schema
    |  name: $name
    |  fields: 
    |    ${fields.joinToString("\n    ")}
    |  primaryKeyFieldNames: [${primaryKeyFieldNames.joinToString(", ")}]
    |]
  """.trimMargin()
}

private fun wrapFieldValue(
  fields: Set<Field>,
  fieldName: String,
  value: Any?,
): Pair<String, Value<*>> {
  val field = fields.find { fld -> fld.name == fieldName }
  if (field == null) {
    throw KDAError.FieldNotFound(
      fieldName = fieldName,
      availableFieldNames = fields.map { it.name }.toSet(),
    )
  } else {
    val wrappedValue = field.dataType.wrapValue(value)
    return fieldName to wrappedValue
  }
}
