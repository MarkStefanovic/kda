package kda.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class RowTest {
  @Test
  fun subset_happy_path() {
    val row =
      Row(
        mapOf(
          "customer_id" to 1,
          "first_name" to "Mark",
          "last_name" to "Stefanovic",
        )
      )
    val subset = row.subset(setOf("first_name", "last_name"))
    val expected =
      Row(
        mapOf(
          "first_name" to "Mark",
          "last_name" to "Stefanovic",
        )
      )
    assertEquals(expected = expected, actual = subset)
  }
}
