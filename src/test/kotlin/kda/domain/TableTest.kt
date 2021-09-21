package kda.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TableTest {
  @Test
  fun row_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(false)),
        Field(name = "first_name", dataType = NullableStringType(null)),
        Field(name = "last_name", dataType = NullableStringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val actualRow = table.row(
      "customer_id" to 1,
      "first_name" to "Mark",
      "last_name" to "Stefanovic",
    )
    val expectedRow = Row(
      mapOf(
        "customer_id" to IntValue(1),
        "first_name" to NullableStringValue("Mark", null),
        "last_name" to NullableStringValue("Stefanovic", null),
      )
    )
    assertEquals(expected = expectedRow, actual = actualRow)
  }

  @Test
  fun rows_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(false)),
        Field(name = "first_name", dataType = NullableStringType(null)),
        Field(name = "last_name", dataType = NullableStringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val actualRows = table.rows(
      mapOf("customer_id" to 1, "first_name" to "Mark", "last_name" to "Stefanovic"),
      mapOf("customer_id" to 2, "first_name" to "Bob", "last_name" to "Smith"),
    )
    val expectedRows = setOf(
      Row(
        mapOf(
          "customer_id" to IntValue(1),
          "first_name" to NullableStringValue("Mark", null),
          "last_name" to NullableStringValue("Stefanovic", null),
        )
      ),
      Row(
        mapOf(
          "customer_id" to IntValue(2),
          "first_name" to NullableStringValue("Bob", null),
          "last_name" to NullableStringValue("Smith", null),
        )
      ),
    )
    assertEquals(expected = expectedRows, actual = actualRows)
  }
}
