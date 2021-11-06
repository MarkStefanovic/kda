package kda.adapter.sqlite

import kda.adapter.JdbcExecutor
import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import kotlin.test.assertEquals

private fun connect() =
  DriverManager.getConnection("jdbc:sqlite:file:test?mode=memory&cache=shared")

class SQLiteInspectorTest {
  @Test
  fun inspectTable_happy_path() {
    connect().use { con ->
      val executor = JdbcExecutor(con = con)
      executor.execute("DROP TABLE IF EXISTS tmp")
      executor.execute(
        """
        CREATE TABLE tmp (
          id INTEGER PRIMARY KEY
        , first_name TEXT NOT NULL
        , last_name TEXT NOT NULL
        , middle_initial CHAR(1) NULL
        , age INT
        )
      """
      )

      val inspector = SQLiteInspector(sqlExecutor = executor)
      val actual = inspector.inspectTable(
        schema = null,
        table = "tmp",
        maxFloatDigits = 4,
        primaryKeyFieldNames = listOf("id"),
      )
      val expected = Table(
        schema = null,
        name = "tmp",
        fields = setOf(
          Field(name = "age", dataType = DataType.int(false)),
          Field(name = "first_name", dataType = DataType.text(null)),
          Field(name = "id", dataType = DataType.int(false)),
          Field(name = "last_name", dataType = DataType.text(null)),
          Field(name = "middle_initial", dataType = DataType.nullableText(1)),
        ),
        primaryKeyFieldNames = listOf("id"),
      )
      assertEquals(expected = expected, actual = actual)
    }
  }

  @Test
  fun tableExists_happy_path() {
    connect().use { con ->
      val executor = JdbcExecutor(con = con)
      executor.execute("DROP TABLE IF EXISTS tmp")
      executor.execute("CREATE TABLE tmp (id INTEGER PRIMARY KEY)")

      val inspector = SQLiteInspector(sqlExecutor = executor)
      val actual = inspector.tableExists(schema = null, table = "tmp")
      assertEquals(expected = true, actual = actual)
    }
  }
}
