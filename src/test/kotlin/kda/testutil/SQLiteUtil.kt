package kda.testutil

import java.sql.Connection
import java.sql.DriverManager

fun testSQLiteDbConnection(): Connection =
  DriverManager.getConnection("jdbc:sqlite:file:test?mode=memory&cache=shared")
