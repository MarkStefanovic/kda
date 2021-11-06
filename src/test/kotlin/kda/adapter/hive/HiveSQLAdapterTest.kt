package kda.adapter.hive

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Row
import kda.domain.Table
import kda.domain.Value
import kda.testutil.standardizeSQL
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class HiveSQLAdapterTest {
  private val adapter = hiveSQLAdapter

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
            dataType = DataType.int(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = DataType.text(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = DataType.text(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val sql = adapter.createTable(table)
    val expected = standardizeSQL(
      """
      CREATE TABLE `sales`.`customer` (
        `customer_id` INT NOT NULL, 
        `first_name` TEXT NULL, 
        `last_name` TEXT NULL, 
        PRIMARY KEY (`customer_id`)
      )
    """
    )
    assertEquals(expected = expected, actual = sql)
  }

  @Test
  fun dropTable_happy_path() {
    val sql = adapter.dropTable(schema = "sales", table = "customer")
    val expected = "DROP TABLE `sales`.`customer`"
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
            dataType = DataType.int(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = DataType.text(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = DataType.text(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to Value.int(1),
          "first_name" to Value.text("Mark"),
          "last_name" to Value.text("Stefanovic"),
        ),
        Row.of(
          "customer_id" to Value.int(2),
          "first_name" to Value.text("Bob"),
          "last_name" to Value.text("Smith"),
        ),
        Row.of(
          "customer_id" to Value.int(3),
          "first_name" to Value.text("Olive"),
          "last_name" to Value.text("Oil"),
        ),
      )
    val sql = adapter.add(table = table, rows = rows)
    val expected = standardizeSQL(
      """
      INSERT INTO `sales`.`customer` (`customer_id`, `first_name`, `last_name`) 
      VALUES 
        (1, 'Mark', 'Stefanovic'), 
        (2, 'Bob', 'Smith'), 
        (3, 'Olive', 'Oil')
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
          Field(name = "customer_id", dataType = DataType.int(autoincrement = true)),
          Field(name = "first_name", dataType = DataType.text(maxLength = 40)),
          Field(name = "last_name", dataType = DataType.text(maxLength = 40)),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to Value.int(1),
          "first_name" to Value.text("Mark"),
          "last_name" to Value.text("Stefanovic"),
        ),
        Row.of(
          "customer_id" to Value.int(2),
          "first_name" to Value.text("Bob"),
          "last_name" to Value.text("Smith"),
        ),
        Row.of(
          "customer_id" to Value.int(3),
          "first_name" to Value.text("Olive"),
          "last_name" to Value.text("Oil"),
        ),
      )
    val sql = adapter.deleteKeys(table = table, primaryKeyValues = rows)
    val expected = "DELETE FROM `sales`.`customer` WHERE `customer_id` IN (1, 2, 3)"
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
          Field(name = "first_name", dataType = DataType.text(maxLength = 40)),
          Field(name = "last_name", dataType = DataType.text(maxLength = 40)),
          Field(name = "age", dataType = DataType.int(autoincrement = true)),
        ),
        primaryKeyFieldNames = listOf("first_name", "last_name"),
      )
    assertEquals(table.primaryKeyFieldNames, listOf("first_name", "last_name"))

    val rows =
      setOf(
        Row.of(
          "first_name" to Value.text("Mark"),
          "last_name" to Value.text("Stefanovic"),
          "age" to Value.int(99)
        ),
        Row.of(
          "first_name" to Value.text("Bob"),
          "last_name" to Value.text("Smith"),
          "age" to Value.int(74)
        ),
      )
    val sql = adapter.deleteKeys(table = table, primaryKeyValues = rows)
    val expected = standardizeSQL(
      """
      WITH d (`first_name`, `last_name`) AS (
        VALUES ('Mark', 'Stefanovic'), 
        ('Bob', 'Smith')
      ) 
      DELETE FROM `sales`.`customer` t 
      USING d 
      WHERE 
        t.`first_name` = d.`first_name` 
        AND t.`last_name` = d.`last_name`
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
            dataType = DataType.int(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = DataType.text(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = DataType.text(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to Value.int(1),
          "first_name" to Value.text("Mark"),
          "last_name" to Value.text("Stefanovic"),
        ),
        Row.of(
          "customer_id" to Value.int(2),
          "first_name" to Value.text("Bob"),
          "last_name" to Value.text("Smith"),
        ),
        Row.of(
          "customer_id" to Value.int(3),
          "first_name" to Value.text("Olive"),
          "last_name" to Value.text("Oil"),
        ),
      )
    val sql = adapter.update(table = table, rows = rows)
    val expected = standardizeSQL(
      """
      WITH u (`customer_id`, `first_name`, `last_name`) AS (
        VALUES 
          (1, 'Mark', 'Stefanovic'), 
          (2, 'Bob', 'Smith'), 
          (3, 'Olive', 'Oil')
      ) 
      UPDATE `sales`.`customer` AS t 
      SET 
        `first_name` = u.`first_name`, 
        `last_name` = u.`last_name` 
      FROM u 
      WHERE 
        t.`customer_id` = u.`customer_id`
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
            dataType = DataType.int(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = DataType.text(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = DataType.text(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("customer_id"),
      )
    val rows =
      setOf(
        Row.of(
          "customer_id" to Value.int(1),
          "first_name" to Value.text("Mark"),
          "last_name" to Value.text("Stefanovic"),
        ),
        Row.of(
          "customer_id" to Value.int(2),
          "first_name" to Value.text("Bob"),
          "last_name" to Value.text("Smith"),
        ),
        Row.of(
          "customer_id" to Value.int(3),
          "first_name" to Value.text("Olive"),
          "last_name" to Value.text("Oil"),
        ),
      )
    val sql = adapter.selectKeys(table = table, primaryKeyValues = rows)
    val expected = standardizeSQL(
      """
      SELECT `customer_id`, `first_name`, `last_name` 
      FROM `sales`.`customer` 
      WHERE `customer_id` IN (1, 2, 3)
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
            dataType = DataType.int(autoincrement = true),
          ),
          Field(
            name = "first_name",
            dataType = DataType.text(maxLength = 40),
          ),
          Field(
            name = "last_name",
            dataType = DataType.text(maxLength = 40),
          ),
        ),
        primaryKeyFieldNames = listOf("first_name", "last_name"),
      )
    val rows =
      setOf(
        Row.of(
          "age" to Value.int(52),
          "first_name" to Value.text("Mark"),
          "last_name" to Value.text("Stefanovic"),
        ),
        Row.of(
          "age" to Value.int(76),
          "first_name" to Value.text("Bob"),
          "last_name" to Value.text("Smith"),
        ),
        Row.of(
          "age" to Value.int(94),
          "first_name" to Value.text("Olive"),
          "last_name" to Value.text("Oil"),
        ),
      )
    val sql = adapter.selectKeys(table = table, primaryKeyValues = rows)
    val expected = standardizeSQL(
      """
      WITH v (`first_name`, `last_name`) AS (
        VALUES 
          ('Mark', 'Stefanovic'), 
          ('Bob', 'Smith'), 
          ('Olive', 'Oil')
      ) 
      SELECT 
        t.`age`, 
        t.`first_name`, 
        t.`last_name` 
      FROM `sales`.`customer` t 
      JOIN v 
        ON t.`first_name` = v.`first_name` 
        AND t.`last_name` = v.`last_name`
    """
    )
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
          dataType = DataType.localDateTime,
        ),
        Field(
          name = "date_updated",
          dataType = DataType.nullableLocalDateTime,
        ),
        Field(
          name = "first_name",
          dataType = DataType.text(maxLength = 40),
        ),
        Field(
          name = "last_name",
          dataType = DataType.text(maxLength = 40),
        ),
      ),
      primaryKeyFieldNames = listOf("first_name", "last_name"),
    )
    val actualSQL = hiveSQLAdapter.selectMaxValues(
      table = table,
      fieldNames = setOf("date_added", "date_updated"),
    )
    val expectedSQL = standardizeSQL(
      """
      SELECT 
        MAX(`date_added`) AS `date_added`, 
        MAX(`date_updated`) AS `date_updated` 
      FROM `sales`.`customer`
    """
    )
    assertEquals(expected = expectedSQL, actual = actualSQL)
  }
}
