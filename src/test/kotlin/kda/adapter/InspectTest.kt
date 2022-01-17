package kda.adapter

import kda.domain.DataType
import kda.domain.Field
import kda.domain.Table
import org.junit.jupiter.api.Test
import testutil.testPgConnection
import kotlin.test.assertEquals

class InspectTest {
  @Test
  fun getPrimaryKeyFields_happy_path() {
    testPgConnection().use { con ->
      con.createStatement().use { statement ->
        statement.execute("DROP TABLE IF EXISTS pk_fields_test")

        statement.execute(
          """
          CREATE TABLE pk_fields_test (
            id SERIAL PRIMARY KEY
          , first_name TEXT NOT NULL 
          , last_name TEXT NOT NULL
          )
        """
        )

        val primaryKeys = getPrimaryKeyFields(
          con = con,
          schema = "public",
          table = "pk_fields_test",
        )

        assertEquals(expected = listOf("id"), actual = primaryKeys)
      }
    }
  }

  @Test
  fun inspectTable_happy_path() {
    testPgConnection().use { con ->
      con.createStatement().use { statement ->
        statement.execute("DROP TABLE IF EXISTS pk_fields_test")

        statement.execute(
          //language=PostgreSQL
          """
          CREATE TABLE pk_fields_test (
            id SERIAL PRIMARY KEY
          , first_name TEXT NOT NULL 
          , last_name TEXT NOT NULL
          , active BOOLEAN NOT NULL
          , superhero_flag BOOLEAN NULL
          , age INT NOT NULL
          , dob DATE NULL
          , hire_date DATE NOT NULL
          , previous_salary DECIMAL(18, 2) NULL
          , salary DECIMAL(18, 2) NOT NULL
          , employee_id BIGINT NOT NULL
          , supervisor_id BIGINT NULL
          , latitude FLOAT NOT NULL
          , longitude FLOAT NULL
          , date_added TIMESTAMP NOT NULL
          )
        """
        )

        val tableDef = inspectTable(
          con = con,
          schema = "public",
          table = "pk_fields_test",
        )

        assertEquals(
          expected = Table(
            name = "pk_fields_test",
            fields = setOf(
              Field(name = "age", dataType = DataType.int),
              Field(name = "dob", dataType = DataType.nullableLocalDate),
              Field(name = "hire_date", dataType = DataType.localDate),
              Field(name = "employee_id", dataType = DataType.bigInt),
              Field(name = "first_name", dataType = DataType.text(null)),
              Field(name = "id", dataType = DataType.int),
              Field(name = "last_name", dataType = DataType.text(null)),
              Field(name = "salary", dataType = DataType.decimal(18, 2)),
              Field(name = "previous_salary", dataType = DataType.nullableDecimal(18, 2)),
              Field(name = "salary", dataType = DataType.decimal(18, 2)),
              Field(name = "active", dataType = DataType.bool),
              Field(name = "superhero_flag", dataType = DataType.nullableBool),
              Field(name = "supervisor_id", dataType = DataType.nullableBigInt),
              Field(name = "latitude", dataType = DataType.float),
              Field(name = "longitude", dataType = DataType.nullableFloat),
              Field(name = "date_added", dataType = DataType.localDateTime),
            ),
            primaryKeyFieldNames = listOf("id"),
          ),
          actual = tableDef,
        )
      }
    }
  }
}
