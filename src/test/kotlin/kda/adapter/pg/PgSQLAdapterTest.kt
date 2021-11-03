package kda.adapter.pg

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import kda.domain.where
import kda.shared.standardizeSQL
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class PgSQLAdapterTest {
  private val adapter = pgSQLAdapter

  @Test
  fun add() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
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
    val actualSQL = adapter.add(table = table, rows = rows)
    val expectedSQL =
      """INSERT INTO "sales"."customer" ("customer_id", "first_name", "last_name") """ +
        """VALUES (1, 'Mark', 'Stefanovic'), (2, 'Mandie', 'Mandlebrot')"""
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun createTable_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val actualSQL = adapter.createTable(table)
    val expectedSQL = standardizeSQL(
      """
      CREATE TABLE "sales"."customer" (
        "customer_id" INT NOT NULL, 
        "first_name" TEXT NULL, 
        "last_name" TEXT NULL, 
        PRIMARY KEY ("customer_id")
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
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
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
    val actualSQL = adapter.deleteKeys(table = table, primaryKeyValues = rows)
    val expectedSQL = """DELETE FROM "sales"."customer" WHERE "customer_id" IN (1, 2)"""
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun dropTable() {
    val actualSQL = adapter.dropTable(schema = "sales", table = "customer")
    val expectedSQL = """DROP TABLE "sales"."customer""""
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun getRowCount_happy_path() {
    val actualSQL = adapter.getRowCount(schema = "sales", table = "customer")
    val expectedSQL = """SELECT COUNT(*) AS "rows" FROM "sales"."customer""""
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }

  @Test
  fun merge_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields = setOf(
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
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
    val actualSQL = adapter.merge(table = table, rows = rows)
    val expectedSQL = standardizeSQL(
      """
      INSERT INTO "sales"."customer" ("customer_id", "first_name", "last_name") 
      VALUES (1, 'Mark', 'Stefanovic'), (2, 'Mandie', 'Mandlebrot') 
      ON CONFLICT ("customer_id") 
      DO UPDATE SET "first_name" = EXCLUDED."first_name", "last_name" = EXCLUDED."last_name"
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
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
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
    val actualSQL = adapter.select(table = table, criteria = criteria, trustPk = true)
    val expectedSQL = standardizeSQL(
      """
      SELECT t."customer_id", t."first_name", t."last_name" 
      FROM "sales"."customer" t
      WHERE "first_name" = 'Bob' OR "last_name" = 'Smith'
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
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val rows = table.rows(
      mapOf("customer_id" to 1),
      mapOf("customer_id" to 2),
      mapOf("customer_id" to 3),
    )
    val actualSQL = adapter.selectKeys(table = table, primaryKeyValues = rows)
    val expectedSQL = standardizeSQL(
      """
      SELECT "customer_id", "first_name", "last_name" 
      FROM "sales"."customer" 
      WHERE "customer_id" IN (1, 2, 3)
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
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "date_added", dataType = DataType.localDateTime),
        Field(name = "date_updated", dataType = DataType.nullableLocalDateTime),
      ),
      primaryKeyFieldNames = listOf("customer_id"),
    )
    val actualSQL = adapter.selectMaxValues(table = table, fieldNames = setOf("date_added", "date_updated"))
    val expectedSQL = standardizeSQL(
      """
      SELECT MAX("date_added") AS "date_added", MAX("date_updated") AS "date_updated" 
      FROM "sales"."customer"
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
        Field(name = "customer_id", dataType = DataType.int(true)),
        Field(name = "first_name", dataType = DataType.text(null)),
        Field(name = "last_name", dataType = DataType.text(null)),
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
    val actualSQL = adapter.update(table = table, rows = rows)
    val expectedSQL = standardizeSQL(
      """
      WITH u ("customer_id", "first_name", "last_name") AS (
        VALUES 
          (1, 'Mark', 'Stefanovic'), 
          (2, 'Mandie', 'Mandlebrot')
      ) 
      UPDATE "sales"."customer" AS t 
      SET 
        "first_name" = u."first_name", 
        "last_name" = u."last_name" 
      FROM u 
      WHERE t."customer_id" = u."customer_id"
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }
}
