package kda.adapter.hive

import kda.adapter.pg.pgSQLAdapter
import kda.domain.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class HiveSQLAdapterTest {
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
    val sql = pgSQLAdapter.createTable(table)
    val expected =
      """CREATE TABLE "sales"."customer" ("customer_id" INT NOT NULL, "first_name" TEXT NULL, "last_name" TEXT NULL, PRIMARY KEY ("customer_id"))"""
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun dropTable_happy_path() {
    val sql = pgSQLAdapter.dropTable(schema = "sales", table = "customer")
    val expected = """DROP TABLE "sales"."customer""""
    assertEquals(expected = expected, actual = sql)
  }

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
    val sql = pgSQLAdapter.add(table = table, rows = rows)
    val expected =
      """INSERT INTO "sales"."customer" ("customer_id", "first_name", "last_name") """ +
        """VALUES (1, 'Mark', 'Stefanovic'), (2, 'Bob', 'Smith'), (3, 'Olive', 'Oil')"""
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
    val sql = pgSQLAdapter.deleteKeys(table = table, primaryKeyValues = rows)
    val expected = """DELETE FROM "sales"."customer" WHERE "customer_id" IN (1, 2, 3)"""
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
    val sql = pgSQLAdapter.deleteKeys(table = table, primaryKeyValues = rows)
    val expected =
      """WITH d ("first_name", "last_name") AS (VALUES ('Mark', 'Stefanovic'), ('Bob', 'Smith')) """ +
        """DELETE FROM "sales"."customer" t USING d WHERE t."first_name" = d."first_name" AND t."last_name" = d."last_name""""
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
    val sql = pgSQLAdapter.update(table = table, rows = rows)
    val expected =
      """WITH u ("customer_id", "first_name", "last_name") AS (VALUES (1, 'Mark', 'Stefanovic'), (2, 'Bob', 'Smith'), (3, 'Olive', 'Oil')) """ +
        """UPDATE "sales"."customer" AS t SET "first_name" = u."first_name", "last_name" = u."last_name" """ +
        """FROM u WHERE t."customer_id" = u."customer_id""""
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
    val sql = pgSQLAdapter.selectKeys(table = table, primaryKeyValues = rows)
    val expected =
      """SELECT "customer_id", "first_name", "last_name" FROM "sales"."customer" WHERE "customer_id" IN (1, 2, 3)"""
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
    val sql = pgSQLAdapter.selectKeys(table = table, primaryKeyValues = rows)
    val expected =
      """WITH v ("first_name", "last_name") AS (VALUES ('Mark', 'Stefanovic'), ('Bob', 'Smith'), ('Olive', 'Oil')) """ +
        """SELECT t."age", t."first_name", t."last_name" """ +
        """FROM "sales"."customer" t """ +
        """JOIN v ON t."first_name" = v."first_name" AND t."last_name" = v."last_name""""
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun maxValues_happy_path() {
    val table = Table(
      schema = "sales",
      name = "customer",
      fields =
      setOf(
        Field(
          name = "date_added",
          dataType = LocalDateType,
        ),
        Field(
          name = "date_updated",
          dataType = NullableLocalDateTimeType,
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
    val actualSQL = hiveSQLAdapter.selectMaxValues(table = table, fieldNames = setOf("date_added", "date_updated"))
    val expectedSQL = "SELECT MAX(`date_added`) AS `date_added`, MAX(`date_updated`) AS `date_updated` FROM `sales`.`customer`"
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }
}
