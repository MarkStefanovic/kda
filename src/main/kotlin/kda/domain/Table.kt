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

        require(primaryKeyFieldNames.count() > 0) {
            "A table must have primary key fields, but no primary key fields were provided."
        }

        require(fields.count() > 0) { "A table must have fields." }
    }

    val sortedFieldNames: List<String> by lazy { fields.map { fld -> fld.name }.sorted() }

//    val sortedPrimaryKeyFieldNames: List<String> by lazy { primaryKeyFieldNames.sorted() }

    fun subset(fieldNames: Set<String>): Table =
        copy(fields = fields.filter { fld -> fld.name in fieldNames}.toSet())
}