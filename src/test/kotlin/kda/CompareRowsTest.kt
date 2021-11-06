package kda

import kda.domain.Dialect
import kda.testutil.DummyCache
import kda.testutil.connectToTestDb
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CompareRowsTest {
  @Test
  fun happy_path() {
    connectToTestDb().use { con ->
      con.prepareStatement(
        "DROP TABLE IF EXISTS customer"
      ).executeUpdate()

      con.prepareStatement(
        "DROP TABLE IF EXISTS customer2"
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer (
          id INTEGER PRIMARY KEY 
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer2 (
          id INTEGER NOT NULL
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (3, 'Bob', NULL, 'Smith')
        , (2, 'Jane', 'D', 'Doe')
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer2 (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (2, 'Jane', 'D', 'Doe')
        , (3, 'Bob', NULL, 'Smith')
      """
      ).executeUpdate()

      val result = compareRows(
        srcCon = con,
        destCon = con,
        srcDialect = Dialect.SQLite,
        destDialect = Dialect.SQLite,
        srcSchema = null,
        srcTable = "customer",
        destSchema = null,
        destTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        cache = DummyCache(),
        showSQL = true,
      ).getOrThrow()

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun given_duplicate_dest_rows() {
    connectToTestDb().use { con ->
      con.prepareStatement(
        "DROP TABLE IF EXISTS customer"
      ).executeUpdate()

      con.prepareStatement(
        "DROP TABLE IF EXISTS customer2"
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer (
          id INTEGER PRIMARY KEY 
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer2 (
          id INTEGER NOT NULL
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (3, 'Bob', NULL, 'Smith')
        , (2, 'Jane', 'D', 'Doe')
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer2 (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (1, 'Mark', 'E', 'Stefanovic')
        , (2, 'Jane', 'D', 'Doe')
        , (3, 'Bob', NULL, 'Smith')
        , (3, 'Bob', NULL, 'Smith')
      """
      ).executeUpdate()

      val result = compareRows(
        srcCon = con,
        destCon = con,
        srcDialect = Dialect.SQLite,
        destDialect = Dialect.SQLite,
        srcSchema = null,
        srcTable = "customer",
        destSchema = null,
        destTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        cache = DummyCache(),
        showSQL = true,
      ).getOrThrow()

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun given_duplicate_src_rows() {
    connectToTestDb().use { con ->
      con.prepareStatement(
        "DROP TABLE IF EXISTS customer"
      ).executeUpdate()

      con.prepareStatement(
        "DROP TABLE IF EXISTS customer2"
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer (
          id INTEGER NOT NULL 
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer2 (
          id INTEGER NOT NULL
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (3, 'Bob', NULL, 'Smith')
        , (3, 'Bob', NULL, 'Smith')
        , (2, 'Jane', 'D', 'Doe')
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer2 (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (1, 'Mark', 'E', 'Stefanovic')
        , (2, 'Jane', 'D', 'Doe')
        , (3, 'Bob', NULL, 'Smith')
        , (3, 'Bob', NULL, 'Smith')
      """
      ).executeUpdate()

      val result = compareRows(
        srcCon = con,
        destCon = con,
        srcDialect = Dialect.SQLite,
        destDialect = Dialect.SQLite,
        srcSchema = null,
        srcTable = "customer",
        destSchema = null,
        destTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        cache = DummyCache(),
        showSQL = true,
      ).getOrThrow()

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun when_rows_are_added() {
    connectToTestDb().use { con ->
      con.prepareStatement(
        "DROP TABLE IF EXISTS customer"
      ).executeUpdate()

      con.prepareStatement(
        "DROP TABLE IF EXISTS customer2"
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer (
          id INTEGER PRIMARY KEY 
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer2 (
          id INTEGER NOT NULL
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (3, 'Bob', NULL, 'Smith')
        , (2, 'Jane', 'D', 'Doe')
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer2 (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (2, 'Jane', 'D', 'Doe')
      """
      ).executeUpdate()

      val result = compareRows(
        srcCon = con,
        destCon = con,
        srcDialect = Dialect.SQLite,
        destDialect = Dialect.SQLite,
        srcSchema = null,
        srcTable = "customer",
        destSchema = null,
        destTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        cache = DummyCache(),
        showSQL = true,
      ).getOrThrow()

      assertEquals(expected = 1, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun when_rows_are_updated() {
    connectToTestDb().use { con ->
      con.prepareStatement(
        "DROP TABLE IF EXISTS customer"
      ).executeUpdate()

      con.prepareStatement(
        "DROP TABLE IF EXISTS customer2"
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer (
          id INTEGER PRIMARY KEY 
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        CREATE TABLE customer2 (
          id INTEGER NOT NULL
        , first_name TEXT NOT NULL
        , mi CHAR(1) NULL 
        , last_name TEXT NOT NULL
        )
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (3, 'Bob', NULL, 'Smith')
        , (2, 'Jane', 'D', 'Doe')
      """
      ).executeUpdate()

      con.prepareStatement(
        """
        INSERT INTO customer2 (id, first_name, mi, last_name) 
        VALUES 
          (1, 'Mark', 'E', 'Stefanovic')
        , (2, 'Jane', 'D', 'Doe')
        , (3, 'Bob', 'B', 'Smith')
      """
      ).executeUpdate()

      val result = compareRows(
        srcCon = con,
        destCon = con,
        srcDialect = Dialect.SQLite,
        destDialect = Dialect.SQLite,
        srcSchema = null,
        srcTable = "customer",
        destSchema = null,
        destTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        cache = DummyCache(),
        showSQL = true,
      ).getOrThrow()

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 1, actual = result.updated.count())
    }
  }
}
