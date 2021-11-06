package kda.adapter.pg

import kda.adapter.JdbcExecutor
import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import kda.testutil.testPgConnection
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class PgInspectorTest {
  @Test
  fun inspectTable_happy_path() {
    testPgConnection().use { con ->
      val executor = JdbcExecutor(con = con)
      executor.execute("DROP TABLE IF EXISTS tmp20210708")
      executor.execute(
        "CREATE TEMP TABLE tmp20210708 (id SERIAL PRIMARY KEY, first_name TEXT NOT NULL, " +
          "last_name TEXT NOT NULL, age INT NOT NULL)"
      )

      val inspector = PgInspector(sqlExecutor = executor)
      val actual = inspector.inspectTable(
        schema = null,
        table = "tmp20210708",
        maxFloatDigits = 4,
        primaryKeyFieldNames = listOf("id"),
      )
      val expected = Table(
        schema = null,
        name = "tmp20210708",
        fields = setOf(
          Field(name = "age", dataType = DataType.int(false)),
          Field(name = "first_name", dataType = DataType.text(null)),
          Field(name = "id", dataType = DataType.int(true)),
          Field(name = "last_name", dataType = DataType.text(null)),
        ),
        primaryKeyFieldNames = listOf("id"),
      )
      assertEquals(expected = expected, actual = actual)
      executor.execute("DROP TABLE tmp20210708")
    }
  }

  @Test
  fun tableExists_happy_path() {
    testPgConnection().use { con ->
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
  fun primaryKeyFields_happy_path() {
    testPgConnection().use { con ->
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
