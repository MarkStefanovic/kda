package kda.domain

import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RowTest {
    @Test
    fun happy_path() {
        val row =
            Row(
                mapOf(
                    "customer_id" to IntValue(1),
                    "first_name" to StringValue(value = "Mark", maxLength = 40),
                    "last_name" to StringValue(value = "Stefanovic", maxLength = 40),
                )
            )
        val subset = row.subset(setOf("first_name", "last_name"))
        val expected =
            Row(
                mapOf(
                    "first_name" to StringValue(value = "Mark", maxLength = 40),
                    "last_name" to StringValue(value = "Stefanovic", maxLength = 40),
                )
            )
        assertEquals(expected = expected, actual = subset)
    }
}
