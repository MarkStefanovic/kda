package kda.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TableTest {
  @Test
  fun row_of_happy_path() {
    val actualRow = Row.of(
      "customer_id" to 1,
      "first_name" to "Mark",
      "last_name" to "Stefanovic",
    )
    val expectedRow = Row(
      mapOf(
        "customer_id" to 1,
        "first_name" to "Mark",
        "last_name" to "Stefanovic",
      )
    )
    assertEquals(expected = expectedRow, actual = actualRow)
  }

  @Test
  fun toString_happy_path() {
    val table = Table(
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = DataType.int),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )

    val expected = """
      |Table [
      |  name: customer
      |  fields: Field [ name: customer_id, dataType: int ], Field [ name: first_name, dataType: text [ maxLength: null ] ], Field [ name: last_name, dataType: text [ maxLength: null ] ]
      |  primaryKeyFieldNames: customer_id
      |]
    """.trimMargin()

    assertEquals(expected = expected, actual = table.toString())
  }
}
