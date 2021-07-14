package adapter

import domain.*
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcExecutorTest {
    private fun connect(): Connection =
        DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/testdb",
            System.getenv("DB_USER"),
            System.getenv("DB_PASS")
        )

    @Test
    fun test_execute() {
        connect().use { con ->
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
    fun test_fetchRow() {
        connect().use { con ->
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
            val actual =
                executor.fetchRow(sql = "SELECT * FROM sales.tmp20210707", fields = table.fields)
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
    fun test_fetchRows() {
        connect().use { con ->
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
                domain.Table(
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
    fun test_fetchNullableBool() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(null as Boolean?, executor.fetchNullableBool("SELECT NULL"))

            assertEquals(true, executor.fetchNullableBool("SELECT TRUE"))

            assertEquals(false, executor.fetchNullableBool("SELECT FALSE"))
        }
    }

    @Test
    fun test_fetchNullableBool_not_a_bool() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<ValueError>("test") { executor.fetchNullableBool("SELECT 'a'") }
        }
    }

    @Test
    fun test_fetchNullableBool_no_rows_returned() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<NoRowsReturned>("test") { executor.fetchNullableBool("SELECT") }
        }
    }

    @Test
    fun test_fetchBool() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(true, executor.fetchBool("SELECT TRUE"))

            assertEquals(false, executor.fetchBool("SELECT FALSE"))
        }
    }

    @Test
    fun test_fetchNullableDate() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(null as LocalDate?, executor.fetchNullableDate("SELECT NULL"))

            assertEquals(
                LocalDate.of(2010, 1, 2),
                executor.fetchNullableDate("SELECT '2010-01-02'::DATE"),
            )
        }
    }

    @Test
    fun test_fetchNullableDate_not_a_date() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<ValueError>("test") { executor.fetchNullableDate("SELECT 'a'") }
        }
    }

    @Test
    fun test_fetchNullableDate_no_rows_returned() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<NoRowsReturned>("test") { executor.fetchNullableDate("SELECT") }
        }
    }

    @Test
    fun test_fetchDate() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(LocalDate.of(2010, 1, 2), executor.fetchDate("SELECT '2010-01-02'::DATE"))
        }
    }

    @Test
    fun test_fetchNullableDateTime() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(null as LocalDateTime?, executor.fetchNullableDateTime("SELECT NULL"))

            assertEquals(
                LocalDateTime.of(2010, 1, 2, 3, 4, 5),
                executor.fetchNullableDateTime("SELECT '2010-01-02 03:04:05'::TIMESTAMP")
            )
        }
    }

    @Test
    fun test_fetchNullableDateTime_not_a_timestamp() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<ValueError>("test") { executor.fetchNullableDateTime("SELECT 'a'") }
        }
    }

    @Test
    fun test_fetchNullableDateTime_no_rows_returned() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<NoRowsReturned>("test") { executor.fetchNullableDateTime("SELECT") }
        }
    }

    @Test
    fun test_fetchDateTime() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(
                LocalDateTime.of(2010, 1, 2, 3, 4, 5),
                executor.fetchDateTime("SELECT '2010-01-02 03:04:05'::TIMESTAMP")
            )
        }
    }

    @Test
    fun test_fetchNullableDecimal() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(null as BigDecimal?, executor.fetchNullableDecimal("SELECT NULL"))

            assertEquals(
                BigDecimal.valueOf(1.234),
                executor.fetchNullableDecimal("SELECT 1.234::DECIMAL(4, 3)")
            )
        }
    }

    @Test
    fun test_fetchNullableDecimal_not_a_decimal() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<ValueError>("test") { executor.fetchNullableDecimal("SELECT 'a'") }
        }
    }

    @Test
    fun test_fetchNullableDecimal_no_rows_returned() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<NoRowsReturned>("test") { executor.fetchNullableDecimal("SELECT") }
        }
    }

    @Test
    fun test_fetchDecimal() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(
                BigDecimal.valueOf(1.234),
                executor.fetchDecimal("SELECT 1.234::DECIMAL(4, 3)")
            )
        }
    }

    @Test
    fun test_fetchNullableInt() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(null as Int?, executor.fetchNullableInt("SELECT NULL"))

            assertEquals(1, executor.fetchNullableInt("SELECT 1"))
        }
    }

    @Test
    fun test_fetchNullableInt_not_a_decimal() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<ValueError>("test") { executor.fetchNullableInt("SELECT 'a'") }
        }
    }

    @Test
    fun test_fetchNullableInt_no_rows_returned() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<NoRowsReturned>("test") { executor.fetchNullableInt("SELECT") }
        }
    }

    @Test
    fun test_fetchInt() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(1, executor.fetchInt("SELECT 1"))
        }
    }

    @Test
    fun test_fetchNullableFloat() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(null as Float?, executor.fetchNullableFloat("SELECT NULL"))

            assertEquals(1.234f, executor.fetchNullableFloat("SELECT 1.234::FLOAT"))
        }
    }

    @Test
    fun test_fetchNullableFloat_not_a_decimal() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<ValueError>("test") { executor.fetchNullableFloat("SELECT 'a'") }
        }
    }

    @Test
    fun test_fetchNullableFloat_no_rows_returned() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<NoRowsReturned>("test") { executor.fetchNullableFloat("SELECT") }
        }
    }

    @Test
    fun test_fetchFloat() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(1.234f, executor.fetchFloat("SELECT 1.234::FLOAT"))
        }
    }

    @Test
    fun test_fetchNullableString() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals(null as String?, executor.fetchNullableString("SELECT NULL"))

            assertEquals("test", executor.fetchNullableString("SELECT 'test'"))
        }
    }

    @Test
    fun test_fetchNullableString_not_a_string() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<ValueError>("test") { executor.fetchNullableString("SELECT 1") }
        }
    }

    @Test
    fun test_fetchNullableString_no_rows_returned() {
        connect().use { con ->
            val executor = JdbcExecutor(con)
            assertFailsWith<NoRowsReturned>("test") { executor.fetchNullableString("SELECT") }
        }
    }

    @Test
    fun test_fetchString() {
        connect().use { con ->
            val executor = JdbcExecutor(con)

            assertEquals("test", executor.fetchString("SELECT 'test'"))
        }
    }
}
