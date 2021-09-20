package kda.adapter.std

import kda.domain.Field
import kda.domain.IntType
import kda.domain.IntValue
import kda.domain.Row
import kda.domain.StringType
import kda.domain.StringValue
import kda.domain.Table
import kda.shared.standardizeSQL
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class StdSQLAdapterTest {
  @Test
  fun add_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(
            name = "customer_id",
            dataType = IntType(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = StringType(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = StringType(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to IntValue(1),
          "first_name" to StringValue("Mark", maxLength = 40),
          "last_name" to StringValue("Stefanovic", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(2),
          "first_name" to StringValue("Bob", maxLength = 40),
          "last_name" to StringValue("Smith", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(3),
          "first_name" to StringValue("Olive", maxLength = 40),
          "last_name" to StringValue("Oil", maxLength = 40),
        ),
      )
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).add(table = table, rows = rows)
    val expected = standardizeSQL(
      """
      INSERT INTO "sales"."customer" ("customer_id", "first_name", "last_name")
      VALUES (1, 'Mark', 'Stefanovic'), (2, 'Bob', 'Smith'), (3, 'Olive', 'Oil')
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun createTable_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(
            name = "customer_id",
            dataType = IntType(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = StringType(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = StringType(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).createTable(table)
    val expected = standardizeSQL(
      """
      CREATE TABLE "sales"."customer" (
        "customer_id" INT NOT NULL,
        "first_name" TEXT NULL,
        "last_name" TEXT NULL,
        PRIMARY KEY ("customer_id")
      )
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun delete_w_single_pk_col_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(name = "customer_id", dataType = IntType(autoincrement = true)),
          Field(name = "first_name", dataType = StringType(maxLength = 40)),
          Field(name = "last_name", dataType = StringType(maxLength = 40)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to IntValue(1),
          "first_name" to StringValue(value = "Mark", maxLength = 40),
          "last_name" to StringValue(value = "Stefanovic", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(2),
          "first_name" to StringValue(value = "Bob", maxLength = 40),
          "last_name" to StringValue(value = "Smith", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(3),
          "first_name" to StringValue(value = "Olive", maxLength = 40),
          "last_name" to StringValue(value = "Oil", maxLength = 40),
        ),
      )
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).deleteKeys(table = table, primaryKeyValues = rows)
    val expected = standardizeSQL(
      """
      DELETE FROM "sales"."customer" 
      WHERE "customer_id" IN (1, 2, 3)
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun delete_w_multi_pk_cols_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(name = "first_name", dataType = StringType(maxLength = 40)),
          Field(name = "last_name", dataType = StringType(maxLength = 40)),
          Field(name = "age", dataType = IntType(autoincrement = true)),
        ),
        primaryKeyFieldNames = listOf("first_name", "last_name"),
      )
    assertEquals(table.primaryKeyFieldNames, listOf("first_name", "last_name"))

    val rows =
      setOf(
        Row.of(
          "first_name" to StringValue("Mark", 40),
          "last_name" to StringValue("Stefanovic", 40),
          "age" to IntValue(99)
        ),
        Row.of(
          "first_name" to StringValue("Bob", 40),
          "last_name" to StringValue("Smith", 40),
          "age" to IntValue(74)
        ),
      )
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).deleteKeys(table = table, primaryKeyValues = rows)
    val expected = standardizeSQL(
      """
        WITH d ("first_name", "last_name") AS (
          VALUES 
            ('Mark', 'Stefanovic'),
            ('Bob', 'Smith')
        )
        DELETE FROM "sales"."customer" t
        USING d 
        WHERE 
          t."first_name" = d."first_name" 
          AND t."last_name" = d."last_name"
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun dropTable_happy_path() {
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).dropTable(schema = "sales", table = "customer")
    val expected = """DROP TABLE "sales"."customer""""
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun merge_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(name = "first_name", dataType = StringType(maxLength = 40)),
          Field(name = "last_name", dataType = StringType(maxLength = 40)),
          Field(name = "age", dataType = IntType(autoincrement = true)),
        ),
        primaryKeyFieldNames = listOf("first_name", "last_name"),
      )
    assertEquals(table.primaryKeyFieldNames, listOf("first_name", "last_name"))

    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).merge(
      table = table,
      rows = setOf(
        Row.of(
          "first_name" to StringValue("Mark", 40),
          "last_name" to StringValue("Stefanovic", 40),
          "age" to IntValue(99)
        ),
        Row.of(
          "first_name" to StringValue("Bob", 40),
          "last_name" to StringValue("Smith", 40),
          "age" to IntValue(74)
        ),
      ),
    )
    val expected = standardizeSQL(
      """
      MERGE INTO "sales"."customer" t
      USING (
        VALUES 
          (99, 'Mark', 'Stefanovic'),
          (74, 'Bob', 'Smith')
      ) v
        ON t."first_name" = v."first_name" 
        AND t."last_name" = v."last_name"
      WHEN NOT MATCHED 
        INSERT VALUES (v."age", v."first_name", v."last_name")
      WHEN MATCHED 
        UPDATE SET "age" = v."age"
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun select_w_single_pk_col_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(
            name = "customer_id",
            dataType = IntType(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = StringType(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = StringType(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to IntValue(1),
          "first_name" to StringValue("Mark", maxLength = 40),
          "last_name" to StringValue("Stefanovic", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(2),
          "first_name" to StringValue("Bob", maxLength = 40),
          "last_name" to StringValue("Smith", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(3),
          "first_name" to StringValue("Olive", maxLength = 40),
          "last_name" to StringValue("Oil", maxLength = 40),
        ),
      )
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).selectKeys(table = table, primaryKeyValues = rows)
    val expected = standardizeSQL(
      """
      SELECT "customer_id", "first_name", "last_name" 
      FROM "sales"."customer" 
      WHERE "customer_id" IN (1, 2, 3)
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun select_w_multi_pk_cols_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(
            name = "age",
            dataType = IntType(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = StringType(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = StringType(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("first_name", "last_name"),
      )
    val rows =
      setOf(
        Row.of(
          "age" to IntValue(52),
          "first_name" to StringValue("Mark", maxLength = 40),
          "last_name" to StringValue("Stefanovic", maxLength = 40),
        ),
        Row.of(
          "age" to IntValue(76),
          "first_name" to StringValue("Bob", maxLength = 40),
          "last_name" to StringValue("Smith", maxLength = 40),
        ),
        Row.of(
          "age" to IntValue(94),
          "first_name" to StringValue("Olive", maxLength = 40),
          "last_name" to StringValue("Oil", maxLength = 40),
        ),
      )
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).selectKeys(table = table, primaryKeyValues = rows)
    val expected = standardizeSQL(
      """
      WITH v ("first_name", "last_name") AS (
        VALUES 
          ('Mark', 'Stefanovic'),
          ('Bob', 'Smith'),
          ('Olive', 'Oil')
      )
      SELECT 
        "age",
        "first_name",
        "last_name"
      FROM "sales"."customer" t
      JOIN v 
        ON t."first_name" = v."first_name" 
        AND t."last_name" = v."last_name"
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun update_happy_path() {
    val table =
      Table(
        schema = "sales",
        name = "customer",
        fields =
        setOf(
          Field(
            name = "customer_id",
            dataType = IntType(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = StringType(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = StringType(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to IntValue(1),
          "first_name" to StringValue("Mark", maxLength = 40),
          "last_name" to StringValue("Stefanovic", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(2),
          "first_name" to StringValue("Bob", maxLength = 40),
          "last_name" to StringValue("Smith", maxLength = 40),
        ),
        Row.of(
          "customer_id" to IntValue(3),
          "first_name" to StringValue("Olive", maxLength = 40),
          "last_name" to StringValue("Oil", maxLength = 40),
        ),
      )
    val sql = StdSQLAdapter(StdSQLAdapterImplDetails()).update(table = table, rows = rows)
    val expected = standardizeSQL(
      """
      WITH u ("customer_id", "first_name", "last_name") AS (
        VALUES
          (1, 'Mark', 'Stefanovic'),
          (2, 'Bob', 'Smith'),
          (3, 'Olive', 'Oil')
      )
      UPDATE "sales"."customer" AS t
      SET
        "first_name" = u."first_name",
        "last_name" = u."last_name"
      FROM u
      WHERE
        t."customer_id" = u."customer_id"
    """
    )
    assertEquals(expected = expected, actual = sql)
  }
}
