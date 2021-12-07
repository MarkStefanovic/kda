package testutil

import java.sql.Connection
import java.sql.DriverManager

fun testSQLiteConnection(): Connection =
  DriverManager.getConnection("jdbc:sqlite:file:test?mode=memory&cache=shared")
