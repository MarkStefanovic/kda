package kda

import kda.domain.DbDialect
import org.junit.jupiter.api.Test
import testutil.testSQLiteConnection
import kotlin.test.assertEquals

@ExperimentalStdlibApi
class CompareRowsTest {
  @Test
  fun happy_path() {
    testSQLiteConnection().use { con ->
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
        dstCon = con,
        cacheCon = con,
        srcDialect = DbDialect.SQLite,
        dstDialect = DbDialect.SQLite,
        cacheDialect = DbDialect.SQLite,
        cacheSchema = null,
        srcSchema = null,
        srcTable = "customer",
        dstSchema = null,
        dstTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        showSQL = true,
        criteria = null,
      )

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun given_duplicate_dest_rows() {
    testSQLiteConnection().use { con ->
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
        dstCon = con,
        cacheCon = con,
        srcDialect = DbDialect.SQLite,
        dstDialect = DbDialect.SQLite,
        cacheDialect = DbDialect.SQLite,
        cacheSchema = null,
        srcSchema = null,
        srcTable = "customer",
        dstSchema = null,
        dstTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        showSQL = true,
      )

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun given_duplicate_src_rows() {
    testSQLiteConnection().use { con ->
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
        dstCon = con,
        cacheCon = con,
        srcDialect = DbDialect.SQLite,
        dstDialect = DbDialect.SQLite,
        cacheDialect = DbDialect.SQLite,
        cacheSchema = null,
        srcSchema = null,
        srcTable = "customer",
        dstSchema = null,
        dstTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        showSQL = true,
      )

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun when_rows_are_added() {
    testSQLiteConnection().use { con ->
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
        dstCon = con,
        cacheCon = con,
        srcDialect = DbDialect.SQLite,
        dstDialect = DbDialect.SQLite,
        cacheDialect = DbDialect.SQLite,
        cacheSchema = null,
        srcSchema = null,
        srcTable = "customer",
        dstSchema = null,
        dstTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        showSQL = true,
      )

      assertEquals(expected = 1, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 0, actual = result.updated.count())
    }
  }

  @Test
  fun when_rows_are_updated() {
    testSQLiteConnection().use { con ->
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
        dstCon = con,
        cacheCon = con,
        srcDialect = DbDialect.SQLite,
        dstDialect = DbDialect.SQLite,
        cacheDialect = DbDialect.SQLite,
        cacheSchema = null,
        srcSchema = null,
        srcTable = "customer",
        dstSchema = null,
        dstTable = "customer2",
        compareFields = setOf("first_name", "mi", "last_name"),
        primaryKeyFieldNames = listOf("id"),
        showSQL = true,
      )

      assertEquals(expected = 0, actual = result.added.count())
      assertEquals(expected = 0, actual = result.deleted.count())
      assertEquals(expected = 1, actual = result.updated.count())
    }
  }
}
