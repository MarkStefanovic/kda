package kda.testutil

import java.sql.Connection
import java.sql.DriverManager

fun connectToTestDb(): Connection =
  DriverManager.getConnection("jdbc:sqlite:file:test?mode=memory&cache=shared")
