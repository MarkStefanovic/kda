package kda.testutil

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
        customer_id INT,
        name TEXT,
        PRIMARY KEY (customer_id)
      )
    """
    val actual = standardizeSQL(sql)
    val expected = "CREATE TABLE sales.customer (customer_id INT, name TEXT, PRIMARY KEY (customer_id))"
    assertEquals(expected = expected, actual = actual)
  }
}
