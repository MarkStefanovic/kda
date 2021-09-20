package kda.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IndexedRowsTest {
  @Test
  fun index_happy_path() {
    val rows = listOf(
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
      Row(
        mapOf(
          "customer_id" to IntValue(3),
          "first_name" to NullableStringValue("Mandie", null),
          "last_name" to NullableStringValue("Mandlebrot", null),
        )
      ),
    )
    val expected = IndexedRows(
      mapOf(
        Row(mapOf("customer_id" to IntValue(1))) to Row(
          mapOf(
            "customer_id" to IntValue(1),
            "first_name" to NullableStringValue("Mark", null),
            "last_name" to NullableStringValue("Stefanovic", null),
          )
        ),
        Row(mapOf("customer_id" to IntValue(2))) to Row(
          mapOf(
            "customer_id" to IntValue(2),
            "first_name" to NullableStringValue("Bob", null),
            "last_name" to NullableStringValue("Smith", null),
          )
        ),
        Row(mapOf("customer_id" to IntValue(3))) to Row(
          mapOf(
            "customer_id" to IntValue(3),
            "first_name" to NullableStringValue("Mandie", null),
            "last_name" to NullableStringValue("Mandlebrot", null),
          )
        ),
      )
    )
    val actual = rows.index("customer_id")
    assertEquals(expected = expected, actual = actual)
  }
}
