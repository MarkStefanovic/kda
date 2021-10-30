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
        Field(name = "customer_id", dataType = DataType.int(false)),
        Field(name = "first_name", dataType = DataType.nullableText(null)),
        Field(name = "last_name", dataType = DataType.nullableText(null)),
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
        "customer_id" to Value.int(1),
        "first_name" to Value.nullableText("Mark"),
        "last_name" to Value.nullableText("Stefanovic"),
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
        Field(name = "customer_id", dataType = DataType.int(false)),
        Field(name = "first_name", dataType = DataType.nullableText(null)),
        Field(name = "last_name", dataType = DataType.nullableText(null)),
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
          "customer_id" to Value.int(1),
          "first_name" to Value.nullableText("Mark"),
          "last_name" to Value.nullableText("Stefanovic"),
        )
      ),
      Row(
        mapOf(
          "customer_id" to Value.int(2),
          "first_name" to Value.nullableText("Bob"),
          "last_name" to Value.nullableText("Smith"),
        )
      ),
    )
    assertEquals(expected = expectedRows, actual = actualRows)
  }
}
