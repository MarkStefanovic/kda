package kda.adapter.mssql

import kda.domain.Field
import kda.domain.IntType
import kda.domain.LocalDateTimeType
import kda.domain.NullableLocalDateTimeType
import kda.domain.StringType
import kda.domain.Table
import kda.domain.where
import kda.shared.standardizeSQL
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class MSSQLAdapterTest {
  @Test
  fun add() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "first_name", dataType = StringType(null)),
        Field(name = "last_name", dataType = StringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val rows = table.rows(
      mapOf(
        "customer_id" to 1,
        "first_name" to "Mark",
        "last_name" to "Stefanovic",
      ),
      mapOf(
        "customer_id" to 2,
        "first_name" to "Mandie",
        "last_name" to "Mandlebrot",
      ),
    )
    val actualSQL = msSQLAdapter.add(table = table, rows = rows)
    val expectedSQL =
      "INSERT INTO [sales].[customer] ([customer_id], [first_name], [last_name]) " +
        "VALUES (1, 'Mark', 'Stefanovic'), (2, 'Mandie', 'Mandlebrot')"
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun createTable_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "first_name", dataType = StringType(null)),
        Field(name = "last_name", dataType = StringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val actualSQL = msSQLAdapter.createTable(table)
    val expectedSQL = standardizeSQL(
      """
      CREATE TABLE [sales].[customer] (
        [customer_id] INT NOT NULL, 
        [first_name] TEXT NULL, 
        [last_name] TEXT NULL, 
        PRIMARY KEY ([customer_id])
      )
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun deleteKeys() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "first_name", dataType = StringType(null)),
        Field(name = "last_name", dataType = StringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val rows = table.rows(
      mapOf(
        "customer_id" to 1,
        "first_name" to "Mark",
        "last_name" to "Stefanovic",
      ),
      mapOf(
        "customer_id" to 2,
        "first_name" to "Mandie",
        "last_name" to "Mandlebrot",
      ),
    )
    val actualSQL = msSQLAdapter.deleteKeys(table = table, primaryKeyValues = rows)
    val expectedSQL = "DELETE FROM [sales].[customer] WHERE [customer_id] IN (1, 2)"
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun dropTable() {
    val actualSQL = msSQLAdapter.dropTable(schema = "sales", table = "customer")
    val expectedSQL = "DROP TABLE [sales].[customer]"
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun getRowCount_happy_path() {
    val actualSQL = msSQLAdapter.getRowCount(schema = "sales", table = "customer")
    val expectedSQL = "SELECT COUNT(*) AS [rows] FROM [sales].[customer]"
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun merge_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "first_name", dataType = StringType(null)),
        Field(name = "last_name", dataType = StringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val rows = table.rows(
      mapOf(
        "customer_id" to 1,
        "first_name" to "Mark",
        "last_name" to "Stefanovic",
      ),
      mapOf(
        "customer_id" to 2,
        "first_name" to "Mandie",
        "last_name" to "Mandlebrot",
      ),
    )
    val actualSQL = msSQLAdapter.merge(table = table, rows = rows)
    val expectedSQL = standardizeSQL(
      """
      WITH v AS (
        SELECT 1 AS [customer_id], 'Mark' AS [first_name], 'Stefanovic' AS [last_name]
        UNION ALL
        SELECT 2 AS [customer_id], 'Mandie' AS [first_name], 'Mandlebrot' AS [last_name]
      )
      MERGE INTO [sales].[customer] t
      USING v ON t.[customer_id] = v.[customer_id]
      WHEN NOT MATCHED
        INSERT VALUES (
          v.[customer_id], 
          v.[first_name], 
          v.[last_name]
        )
      WHEN MATCHED 
        UPDATE SET
          [first_name] = v.[first_name], 
          [last_name] = v.[last_name]
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun select_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "first_name", dataType = StringType(null)),
        Field(name = "last_name", dataType = StringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val criteria = where {
      textField("first_name") {
        eq("Bob")
      }
      textField("last_name") {
        eq("Smith")
      }
    }
    assertFalse(criteria.isEmpty())
    val actualSQL = msSQLAdapter.select(table = table, criteria = criteria)
    val expectedSQL = standardizeSQL(
      """
      SELECT [customer_id], [first_name], [last_name] FROM [sales].[customer] 
      WHERE ([first_name] = 'Bob') OR ([last_name] = 'Smith')
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun selectKeys_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "first_name", dataType = StringType(null)),
        Field(name = "last_name", dataType = StringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val rows = table.rows(
      mapOf("customer_id" to 1),
      mapOf("customer_id" to 2),
      mapOf("customer_id" to 3),
    )
    val actualSQL = msSQLAdapter.selectKeys(table = table, primaryKeyValues = rows)
    val expectedSQL = standardizeSQL(
      """
      SELECT [customer_id], [first_name], [last_name] 
      FROM [sales].[customer] 
      WHERE [customer_id] IN (1, 2, 3)
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun selectMaxValues_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "date_added", dataType = LocalDateTimeType),
        Field(name = "date_updated", dataType = NullableLocalDateTimeType),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val actualSQL = msSQLAdapter.selectMaxValues(table = table, fieldNames = setOf("date_added", "date_updated"))
    val expectedSQL = standardizeSQL(
      """
      SELECT MAX([date_added]) AS [date_added], MAX([date_updated]) AS [date_updated] 
      FROM [sales].[customer]
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun update_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = IntType(true)),
        Field(name = "first_name", dataType = StringType(null)),
        Field(name = "last_name", dataType = StringType(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val rows = table.rows(
      mapOf(
        "customer_id" to 1,
        "first_name" to "Mark",
        "last_name" to "Stefanovic",
      ),
      mapOf(
        "customer_id" to 2,
        "first_name" to "Mandie",
        "last_name" to "Mandlebrot",
      ),
    )
    val actualSQL = msSQLAdapter.update(table = table, rows = rows)
    val expectedSQL = standardizeSQL(
      """
      WITH u AS (
        SELECT 1 AS [customer_id], 'Mark' AS [first_name], 'Stefanovic' AS [last_name] 
        UNION ALL 
        SELECT 2 AS [customer_id], 'Mandie' AS [first_name], 'Mandlebrot' AS [last_name]
      ) 
      UPDATE [sales].[customer] AS t 
      SET [first_name] = u.[first_name], [last_name] = u.[last_name] 
      FROM u 
      WHERE t.[customer_id] = u.[customer_id]
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }
}