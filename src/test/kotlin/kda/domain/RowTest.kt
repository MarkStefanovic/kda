package kda.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class RowTest {
  @Test
  fun happy_path() {
    val row =
      Row(
        mapOf(
          "customer_id" to Value.int(1),
          "first_name" to Value.text(value = "Mark"),
          "last_name" to Value.text(value = "Stefanovic"),
        )
      )
    val subset = row.subset(setOf("first_name", "last_name"))
    val expected =
      Row(
        mapOf(
          "first_name" to Value.text(value = "Mark"),
          "last_name" to Value.text(value = "Stefanovic"),
        )
      )
    assertEquals(expected = expected, actual = subset)
  }
}
