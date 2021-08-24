package kda.domain

interface Inspector {
  fun inspectTable(schema: String?, table: String, maxFloatDigits: Int): Table

  fun primaryKeyFields(schema: String?, table: String): List<String>

  fun tableExists(schema: String?, table: String): Boolean
}
