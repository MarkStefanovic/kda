package kda.shared

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

fun standardizeSQL(sql: String) =
  sql
    .split("\n")
    .joinToString(" ")
    .replace("\\s+".toRegex(), " ")
    .replace("( ", "(")
    .replace(" )", ")")
    .trim()

class StandardizeSQLTest {
  @Test
  fun standardizeSQL_happy_path() {
    val sql = """
      CREATE TABLE sales.customer (
        customer_id SERIAL PRIMARY KEY,
        name TEXT 
      )
    """
    val actual = standardizeSQL(sql)
    val expected = "CREATE TABLE sales.customer (customer_id SERIAL PRIMARY KEY, name TEXT)"
    assertEquals(expected = expected, actual = actual)
  }
}
