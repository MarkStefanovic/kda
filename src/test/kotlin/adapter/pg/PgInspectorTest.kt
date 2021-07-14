package adapter.pg

import adapter.JdbcExecutor
import domain.*
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PgInspectorTest {
    private fun connect(): Connection =
        DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/testdb",
            System.getenv("DB_USER"),
            System.getenv("DB_PASS")
        )

    @Test
    fun test_inspectTable() {
        connect().use { con ->
            val executor = JdbcExecutor(con = con)
            executor.execute("DROP TABLE IF EXISTS tmp20210708")
            executor.execute(
                "CREATE TEMP TABLE tmp20210708 (id SERIAL PRIMARY KEY, first_name TEXT NOT NULL, " +
                    "last_name TEXT NOT NULL, age INT NOT NULL)"
            )

            val inspector = PgInspector(sqlExecutor = executor)
            val actual =
                inspector.inspectTable(schema = null, table = "tmp20210708", maxFloatDigits = 4)
            val expected =
                Table(
                    schema = null,
                    name = "tmp20210708",
                    fields =
                        setOf(
                            Field(name = "age", dataType = IntType(autoincrement = false)),
                            Field(name = "first_name", dataType = StringType(maxLength = null)),
                            Field(name = "id", dataType = IntType(autoincrement = true)),
                            Field(name = "last_name", dataType = StringType(maxLength = null)),
                        ),
                    primaryKeyFieldNames = listOf("id"),
                )
            assertEquals(expected = expected, actual = actual)
            executor.execute("DROP TABLE tmp20210708")
        }
    }

    @Test
    fun test_tableExists() {
        connect().use { con ->
            val executor = JdbcExecutor(con = con)
            executor.execute("DROP TABLE IF EXISTS tmp20210708")
            executor.execute("CREATE TEMP TABLE tmp20210708 (id SERIAL PRIMARY KEY)")

            val inspector = PgInspector(sqlExecutor = executor)
            val actual = inspector.tableExists(schema = null, table = "tmp20210708")
            assertEquals(expected = true, actual = actual)
            executor.execute("DROP TABLE tmp20210708")
        }
    }

    @Test
    fun test_primaryKeyFields() {
        connect().use { con ->
            val executor = JdbcExecutor(con = con)
            executor.execute("DROP TABLE IF EXISTS tmp20210708")
            executor.execute("CREATE TEMP TABLE tmp20210708 (id SERIAL PRIMARY KEY)")

            val inspector = PgInspector(sqlExecutor = executor)
            val actual = inspector.primaryKeyFields(schema = null, table = "tmp20210708")
            assertEquals(expected = listOf("id"), actual = actual)
            executor.execute("DROP TABLE tmp20210708")
        }
    }
}
