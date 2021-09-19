package kda.adapter

import kda.domain.Field
import kda.domain.IntType
import kda.domain.IntValue
import kda.domain.KDAError
import kda.domain.Row
import kda.domain.StringType
import kda.domain.StringValue
import kda.domain.Table
import kda.shared.testPgConnection
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcExecutorTest {
  @Test
  fun execute_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      executor.execute("DROP TABLE IF EXISTS tmp20210707")
      executor.execute("CREATE TEMP TABLE tmp20210707 (id INT PRIMARY KEY)")
      executor.execute("INSERT INTO tmp20210707 (id) VALUES (1)")

      val result = executor.fetchInt("SELECT id FROM tmp20210707")
      assertEquals(expected = 1, actual = result)

      executor.execute("DROP TABLE tmp20210707")
    }
  }

  @Test
  fun fetchRow_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      executor.execute("DROP TABLE IF EXISTS sales.tmp20210707")
      executor.execute(
        "CREATE TABLE sales.tmp20210707 (id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL)"
      )
      executor.execute(
        "INSERT INTO sales.tmp20210707 (id, first_name, last_name) VALUES (1, 'Mark', 'Stefanovic')"
      )

      val table =
        Table(
          schema = "sales",
          name = "tmp20210707",
          fields =
          setOf(
            Field(
              name = "id",
              dataType = IntType(autoincrement = true),
            ),
            Field(
              name = "first_name",
              dataType = StringType(maxLength = 40),
            ),
            Field(
              name = "last_name",
              dataType = StringType(maxLength = 40),
            )
          ),
          primaryKeyFieldNames = listOf("id"),
        )
      val actual = executor.fetchRow(sql = "SELECT * FROM sales.tmp20210707", fields = table.fields)
      val expected =
        Row.of(
          "id" to IntValue(1),
          "first_name" to StringValue("Mark", maxLength = 40),
          "last_name" to StringValue("Stefanovic", maxLength = 40),
        )
      assertEquals(expected = expected, actual = actual)
      executor.execute("DROP TABLE sales.tmp20210707")
    }
  }

  @Test
  fun fetchRows_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      executor.execute("DROP TABLE IF EXISTS sales.tmp20210707")
      executor.execute(
        "CREATE TABLE sales.tmp20210707 (id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL)"
      )
      executor.execute(
        """
                INSERT INTO sales.tmp20210707 (id, first_name, last_name) 
                VALUES (1, 'Mark', 'Stefanovic'), (2, 'Bob', 'Smith'), (3, 'Olive', 'Oil')
            """
      )

      val table =
        kda.domain.Table(
          schema = "sales",
          name = "tmp20210707",
          fields =
          setOf(
            Field(
              name = "id",
              dataType = IntType(autoincrement = true),
            ),
            Field(
              name = "first_name",
              dataType = StringType(maxLength = 40),
            ),
            Field(
              name = "last_name",
              dataType = StringType(maxLength = 40),
            )
          ),
          primaryKeyFieldNames = listOf("id"),
        )
      val actual =
        executor.fetchRows(sql = "SELECT * FROM sales.tmp20210707", fields = table.fields)
      val expected =
        setOf(
          Row.of(
            "id" to IntValue(1),
            "first_name" to StringValue("Mark", maxLength = 40),
            "last_name" to StringValue("Stefanovic", maxLength = 40),
          ),
          Row.of(
            "id" to IntValue(2),
            "first_name" to StringValue("Bob", maxLength = 40),
            "last_name" to StringValue("Smith", maxLength = 40),
          ),
          Row.of(
            "id" to IntValue(3),
            "first_name" to StringValue("Olive", maxLength = 40),
            "last_name" to StringValue("Oil", maxLength = 40),
          ),
        )
      assertEquals(expected = expected, actual = actual)
      executor.execute("DROP TABLE sales.tmp20210707")
    }
  }

  @Test
  fun fetchNullableBool() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(null as Boolean?, executor.fetchNullableBool("SELECT NULL"))

      assertEquals(true, executor.fetchNullableBool("SELECT TRUE"))

      assertEquals(false, executor.fetchNullableBool("SELECT FALSE"))
    }
  }

  @Test
  fun fetchNullableBool_not_a_bool() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.ValueError>("test") { executor.fetchNullableBool("SELECT 'a'") }
    }
  }

  @Test
  fun fetchNullableBool_no_rows_returned() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.NoRowsReturned>("test") { executor.fetchNullableBool("SELECT") }
    }
  }

  @Test
  fun fetchBool_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(true, executor.fetchBool("SELECT TRUE"))

      assertEquals(false, executor.fetchBool("SELECT FALSE"))
    }
  }

  @Test
  fun fetchNullableDate_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(null as LocalDate?, executor.fetchNullableDate("SELECT NULL"))

      assertEquals(
        LocalDate.of(2010, 1, 2),
        executor.fetchNullableDate("SELECT '2010-01-02'::DATE"),
      )
    }
  }

  @Test
  fun fetchNullableDate_not_a_date() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.ValueError>("test") { executor.fetchNullableDate("SELECT 'a'") }
    }
  }

  @Test
  fun fetchNullableDate_no_rows_returned() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.NoRowsReturned>("test") { executor.fetchNullableDate("SELECT") }
    }
  }

  @Test
  fun fetchDate_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(LocalDate.of(2010, 1, 2), executor.fetchDate("SELECT '2010-01-02'::DATE"))
    }
  }

  @Test
  fun fetchNullableDateTime_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(null as LocalDateTime?, executor.fetchNullableDateTime("SELECT NULL"))

      assertEquals(
        LocalDateTime.of(2010, 1, 2, 3, 4, 5),
        executor.fetchNullableDateTime("SELECT '2010-01-02 03:04:05'::TIMESTAMP")
      )
    }
  }

  @Test
  fun fetchNullableDateTime_not_a_timestamp() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.ValueError>("test") { executor.fetchNullableDateTime("SELECT 'a'") }
    }
  }

  @Test
  fun fetchNullableDateTime_no_rows_returned() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.NoRowsReturned>("test") { executor.fetchNullableDateTime("SELECT") }
    }
  }

  @Test
  fun fetchDateTime_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(
        LocalDateTime.of(2010, 1, 2, 3, 4, 5),
        executor.fetchDateTime("SELECT '2010-01-02 03:04:05'::TIMESTAMP")
      )
    }
  }

  @Test
  fun fetchNullableDecimal_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(null as BigDecimal?, executor.fetchNullableDecimal("SELECT NULL"))

      assertEquals(
        BigDecimal.valueOf(1.234),
        executor.fetchNullableDecimal("SELECT 1.234::DECIMAL(4, 3)")
      )
    }
  }

  @Test
  fun fetchNullableDecimal_not_a_decimal() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.ValueError>("test") { executor.fetchNullableDecimal("SELECT 'a'") }
    }
  }

  @Test
  fun fetchNullableDecimal_no_rows_returned() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.NoRowsReturned>("test") { executor.fetchNullableDecimal("SELECT") }
    }
  }

  @Test
  fun fetchDecimal() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(BigDecimal.valueOf(1.234), executor.fetchDecimal("SELECT 1.234::DECIMAL(4, 3)"))
    }
  }

  @Test
  fun fetchNullableInt() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(null as Int?, executor.fetchNullableInt("SELECT NULL"))

      assertEquals(1, executor.fetchNullableInt("SELECT 1"))
    }
  }

  @Test
  fun fetchNullableInt_not_a_decimal() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.ValueError>("test") { executor.fetchNullableInt("SELECT 'a'") }
    }
  }

  @Test
  fun fetchNullableInt_no_rows_returned() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.NoRowsReturned>("test") { executor.fetchNullableInt("SELECT") }
    }
  }

  @Test
  fun fetchInt_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(1, executor.fetchInt("SELECT 1"))
    }
  }

  @Test
  fun fetchNullableFloat() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(null as Float?, executor.fetchNullableFloat("SELECT NULL", 5))

      assertEquals(1.234f, executor.fetchNullableFloat("SELECT 1.234::FLOAT", 5))
    }
  }

  @Test
  fun fetchNullableFloat_not_a_decimal() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.ValueError>("test") { executor.fetchNullableFloat("SELECT 'a'", 5) }
    }
  }

  @Test
  fun fetchNullableFloat_no_rows_returned() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.NoRowsReturned>("test") { executor.fetchNullableFloat("SELECT", 5) }
    }
  }

  @Test
  fun fetchFloat_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(1.234f, executor.fetchFloat("SELECT 1.234::FLOAT", 5))
    }
  }

  @Test
  fun fetchNullableString_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals(null as String?, executor.fetchNullableString("SELECT NULL", null))

      assertEquals("test", executor.fetchNullableString("SELECT 'test'", null))
    }
  }

  @Test
  fun fetchNullableString_not_a_string() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.ValueError>("test") { executor.fetchNullableString("SELECT 1", null) }
    }
  }

  @Test
  fun fetchNullableString_no_rows_returned() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)
      assertFailsWith<KDAError.NoRowsReturned>("test") { executor.fetchNullableString("SELECT", null) }
    }
  }

  @Test
  fun fetchString_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con)

      assertEquals("test", executor.fetchString("SELECT 'test'", null))
    }
  }
}
