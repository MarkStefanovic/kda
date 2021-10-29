package kda.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IndexedRowsTest {
  @Test
  fun index_happy_path() {
    val rows = listOf(
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
      Row(
        mapOf(
          "customer_id" to Value.int(3),
          "first_name" to Value.nullableText("Mandie"),
          "last_name" to Value.nullableText("Mandlebrot"),
        )
      ),
    )
    val expected = IndexedRows(
      mapOf(
        Row(mapOf("customer_id" to Value.int(1))) to Row(
          mapOf(
            "customer_id" to Value.int(1),
            "first_name" to Value.nullableText("Mark"),
            "last_name" to Value.nullableText("Stefanovic"),
          )
        ),
        Row(mapOf("customer_id" to Value.int(2))) to Row(
          mapOf(
            "customer_id" to Value.int(2),
            "first_name" to Value.nullableText("Bob"),
            "last_name" to Value.nullableText("Smith"),
          )
        ),
        Row(mapOf("customer_id" to Value.int(3))) to Row(
          mapOf(
            "customer_id" to Value.int(3),
            "first_name" to Value.nullableText("Mandie"),
            "last_name" to Value.nullableText("Mandlebrot"),
          )
        ),
      )
    )
    val actual = rows.index("customer_id")
    assertEquals(expected = expected, actual = actual)
  }
}
