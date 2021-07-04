package domain

data class Table (
    val schema: String,
    val name: String,
    val fields: Set<Field>,
    private val primaryKeyFields: List<String>,
) {
    val sortedFieldNames: List<String> by lazy {
        fields.map { fld -> fld.name }.sorted()
    }

    val sortedPrimaryKeyFieldNames: List<String> by lazy {
        primaryKeyFields.sorted()
    }
}
