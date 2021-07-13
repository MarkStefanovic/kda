package adapter.std

import adapter.pg.pgSQLAdapter
import domain.*
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StdSQLAdapterTest {
    @Test
    fun test_createTable() {
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
                primaryKeyFields = listOf("customer_id"),
            )
        val sql = pgSQLAdapter.createTable(table)
        val expected =
            "CREATE TABLE sales.customer (customer_id INT NOT NULL, first_name TEXT NULL, " +
                "last_name TEXT NULL, PRIMARY KEY (customer_id))"
        assertEquals(expected = expected, actual = sql)
    }

    @Test
    fun test_dropTable() {
        val sql = pgSQLAdapter.dropTable(schema = "sales", table = "customer")
        val expected = "DROP TABLE sales.customer"
        assertEquals(expected = expected, actual = sql)
    }

    @Test
    fun test_add() {
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
                primaryKeyFields = listOf("customer_id"),
            )
        val rows =
            IndexedRows.of(
                Row.of("customer_id" to IntValue(1)) to
                    Row.of(
                        "customer_id" to IntValue(1),
                        "first_name" to StringValue("Mark", maxLength = 40),
                        "last_name" to StringValue("Stefanovic", maxLength = 40),
                    ),
                Row.of("customer_id" to IntValue(2)) to
                    Row.of(
                        "customer_id" to IntValue(2),
                        "first_name" to StringValue("Bob", maxLength = 40),
                        "last_name" to StringValue("Smith", maxLength = 40),
                    ),
                Row.of("customer_id" to IntValue(3)) to
                    Row.of(
                        "customer_id" to IntValue(3),
                        "first_name" to StringValue("Olive", maxLength = 40),
                        "last_name" to StringValue("Oil", maxLength = 40),
                    ),
            )
        val sql = pgSQLAdapter.add(table = table, rows = rows)
        val expected =
            "INSERT INTO sales.customer (customer_id, first_name, last_name) " +
                "VALUES (1, 'Mark', 'Stefanovic'), (2, 'Bob', 'Smith'), (3, 'Olive', 'Oil')"
        assertEquals(expected = expected, actual = sql)
    }

    @Test
    fun test_delete_w_single_pk_col() {
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
                primaryKeyFields = listOf("customer_id"),
            )
        val rows =
            IndexedRows.of(
                Row.of("customer_id" to IntValue(1)) to
                    Row.of(
                        "customer_id" to IntValue(1),
                        "first_name" to StringValue(value = "Mark", maxLength = 40),
                        "last_name" to StringValue(value = "Stefanovic", maxLength = 40),
                    ),
                Row.of("customer_id" to IntValue(2)) to
                    Row.of(
                        "customer_id" to IntValue(2),
                        "first_name" to StringValue(value = "Bob", maxLength = 40),
                        "last_name" to StringValue(value = "Smith", maxLength = 40),
                    ),
                Row.of("customer_id" to IntValue(3)) to
                    Row.of(
                        "customer_id" to IntValue(3),
                        "first_name" to StringValue(value = "Olive", maxLength = 40),
                        "last_name" to StringValue(value = "Oil", maxLength = 40),
                    ),
            )
        val sql = pgSQLAdapter.delete(table = table, primaryKeyValues = rows)
        val expected = "DELETE FROM sales.customer WHERE customer_id IN (1, 2, 3)"
        assertEquals(expected = expected, actual = sql)
    }

    @Test
    fun test_delete_w_multi_pk_cols() {
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
                primaryKeyFields = listOf("first_name", "last_name"),
            )
        assertEquals(table.sortedPrimaryKeyFieldNames, listOf("first_name", "last_name"))

        val rows =
            IndexedRows.of(
                Row.of(
                    "first_name" to StringValue("Mark", 40),
                    "last_name" to StringValue("Stefanovic", 40),
                ) to
                    Row.of(
                        "first_name" to StringValue("Mark", 40),
                        "last_name" to StringValue("Stefanovic", 40),
                        "age" to IntValue(99)
                    ),
                Row.of(
                    "first_name" to StringValue("Bob", 40),
                    "last_name" to StringValue("Smith", 40),
                ) to
                    Row.of(
                        "first_name" to StringValue("Bob", 40),
                        "last_name" to StringValue("Smith", 40),
                        "age" to IntValue(74)
                    ),
            )
        val sql = pgSQLAdapter.delete(table = table, primaryKeyValues = rows)
        val expected =
            "WITH d (first_name, last_name) AS (VALUES ('Mark', 'Stefanovic'), ('Bob', 'Smith')) " +
                "DELETE FROM sales.customer t " +
                "USING d WHERE t.first_name = d.first_name AND t.last_name = d.last_name"
        assertEquals(expected = expected, actual = sql)
    }

    @Test
    fun test_update() {
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
                primaryKeyFields = listOf("customer_id"),
            )
        val rows =
            IndexedRows.of(
                Row.of("id" to IntValue(1)) to
                    Row.of(
                        "customer_id" to IntValue(1),
                        "first_name" to StringValue("Mark", maxLength = 40),
                        "last_name" to StringValue("Stefanovic", maxLength = 40),
                    ),
                Row.of("id" to IntValue(2)) to
                    Row.of(
                        "customer_id" to IntValue(2),
                        "first_name" to StringValue("Bob", maxLength = 40),
                        "last_name" to StringValue("Smith", maxLength = 40),
                    ),
                Row.of("id" to IntValue(3)) to
                    Row.of(
                        "customer_id" to IntValue(3),
                        "first_name" to StringValue("Olive", maxLength = 40),
                        "last_name" to StringValue("Oil", maxLength = 40),
                    ),
            )
        val sql = pgSQLAdapter.update(table = table, rows = rows)
        val expected =
            "WITH u (customer_id, first_name, last_name) AS " +
                "(VALUES (1, 'Mark', 'Stefanovic'), (2, 'Bob', 'Smith'), (3, 'Olive', 'Oil')) " +
                "UPDATE sales.customer AS t SET t.first_name = u.first_name, t.last_name = u.last_name " +
                "FROM u ON t.customer_id = u.customer_id"
        assertEquals(expected = expected, actual = sql)
    }

    @Test
    fun test_select_w_single_pk_col() {
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
                primaryKeyFields = listOf("customer_id"),
            )
        val rows =
            IndexedRows.of(
                Row.of("customer_id" to IntValue(1)) to
                    Row.of(
                        "customer_id" to IntValue(1),
                        "first_name" to StringValue("Mark", maxLength = 40),
                        "last_name" to StringValue("Stefanovic", maxLength = 40),
                    ),
                Row.of("customer_id" to IntValue(2)) to
                    Row.of(
                        "customer_id" to IntValue(2),
                        "first_name" to StringValue("Bob", maxLength = 40),
                        "last_name" to StringValue("Smith", maxLength = 40),
                    ),
                Row.of("customer_id" to IntValue(3)) to
                    Row.of(
                        "customer_id" to IntValue(3),
                        "first_name" to StringValue("Olive", maxLength = 40),
                        "last_name" to StringValue("Oil", maxLength = 40),
                    ),
            )
        val sql = pgSQLAdapter.selectKeys(table = table, primaryKeyValues = rows)
        val expected =
            "SELECT customer_id, first_name, last_name FROM sales.customer WHERE customer_id IN (1, 2, 3)"
        assertEquals(expected = expected, actual = sql)
    }

    @Test
    fun test_select_w_multi_pk_cols() {
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
                primaryKeyFields = listOf("first_name", "last_name"),
            )
        val rows =
            IndexedRows.of(
                Row.of("id" to IntValue(1)) to
                    Row.of(
                        "age" to IntValue(52),
                        "first_name" to StringValue("Mark", maxLength = 40),
                        "last_name" to StringValue("Stefanovic", maxLength = 40),
                    ),
                Row.of("id" to IntValue(2)) to
                    Row.of(
                        "age" to IntValue(76),
                        "first_name" to StringValue("Bob", maxLength = 40),
                        "last_name" to StringValue("Smith", maxLength = 40),
                    ),
                Row.of("id" to IntValue(3)) to
                    Row.of(
                        "age" to IntValue(94),
                        "first_name" to StringValue("Olive", maxLength = 40),
                        "last_name" to StringValue("Oil", maxLength = 40),
                    ),
            )
        val sql = pgSQLAdapter.selectKeys(table = table, primaryKeyValues = rows)
        val expected =
            "WITH v (first_name, last_name) AS (VALUES ('Mark', 'Stefanovic'), ('Bob', 'Smith'), ('Olive', 'Oil')) " +
                "SELECT age, first_name, last_name " +
                "FROM sales.customer t " +
                "JOIN v ON t.first_name = d.first_name AND t.last_name = d.last_name"
        assertEquals(expected = expected, actual = sql)
    }
}
