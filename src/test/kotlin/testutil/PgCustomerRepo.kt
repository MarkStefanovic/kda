@file:Suppress("SqlResolve")

package testutil

import java.sql.Connection
import java.sql.Timestamp
import java.sql.Types

class PgCustomerRepo(
  private val con: Connection,
  private val tableName: String,
) {
  fun addUniqueConstraint() {
    con.createStatement().use { statement ->
      statement.execute(
        // language=PostgreSQL
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_customer_id ON sales.$tableName (customer_id)"
      )
    }
  }

  fun fetchCustomers(): Set<Customer> {
    // language=PostgreSQL
    val sql = """
    |  SELECT customer_id, first_name, last_name, middle_initial, date_added, date_updated
    |  FROM sales.$tableName 
    |  ORDER BY customer_id
    """.trimMargin()
    println(
      """
    |SyncTest.fetchCustomers SQL:
    |$sql
      """.trimMargin()
    )

    val customers = mutableListOf<Customer>()
    con.createStatement().use { stmt ->
      stmt.executeQuery(sql).use { rs ->
        while (rs.next()) {
          val dateUpdatedObj = rs.getObject("date_updated")
          val dateUpdated = if (dateUpdatedObj == null) {
            null
          } else {
            (dateUpdatedObj as Timestamp).toLocalDateTime()
          }
          val customer =
            Customer(
              customerId = rs.getInt("customer_id"),
              firstName = rs.getString("first_name"),
              lastName = rs.getString("last_name"),
              middleInitial = rs.getObject("middle_initial") as String?,
              dateAdded = rs.getTimestamp("date_added").toLocalDateTime(),
              dateUpdated = dateUpdated,
            )
          customers.add(customer)
        }
      }
    }
    return customers.toSet()
  }

  fun addCustomers(vararg customers: Customer) {
    for (customer in customers) {
      // language=PostgreSQL
      val sql = """
      |    INSERT INTO sales.$tableName (customer_id, first_name, last_name, middle_initial, date_added, date_updated)
      |    VALUES (?, ?, ?, ?, ?, ?)
      """.trimMargin()
      println(
        """
      |SyncTest.addCustomers:
      |  SQL:
      |$sql
      |  Parameters:
      |    ${customers.joinToString("\n    ")}
        """.trimMargin()
      )

      con.prepareStatement(sql).use { stmt ->
        stmt.setInt(1, customer.customerId)
        stmt.setString(2, customer.firstName)
        stmt.setString(3, customer.lastName)
        if (customer.middleInitial == null) {
          stmt.setNull(4, Types.VARCHAR)
        } else {
          stmt.setString(4, customer.middleInitial)
        }
        stmt.setTimestamp(5, Timestamp.valueOf(customer.dateAdded))
        if (customer.dateUpdated == null) {
          stmt.setNull(6, Types.TIMESTAMP)
        } else {
          stmt.setTimestamp(6, Timestamp.valueOf(customer.dateUpdated))
        }
        stmt.execute()
      }
    }
  }

  fun deleteCustomer(customerId: Int) {
    // language=PostgreSQL
    val sql = """
      DELETE FROM sales.$tableName 
      WHERE customer_id = ?
    """
    con.prepareStatement(sql).use { stmt ->
      stmt.setInt(1, customerId)
      stmt.execute()
    }
  }

  fun recreateCustomerTable() {
    con.createStatement().use { stmt ->
      // language=PostgreSQL
      stmt.execute("DROP TABLE IF EXISTS sales.customer")
      stmt.execute(
        // language=PostgreSQL
        """
          CREATE TABLE sales.$tableName (
              customer_id INT NOT NULL
          ,   first_name TEXT NOT NULL
          ,   last_name TEXT NOT NULL
          ,   middle_initial TEXT NULL
          ,   date_added TIMESTAMP NOT NULL DEFAULT now()
          ,   date_updated TIMESTAMP NULL
          )
          """
      )
    }
  }

  fun recreateCustomer2Table() {
    con.createStatement().use { stmt ->
      // language=PostgreSQL
      stmt.execute("DROP TABLE IF EXISTS sales.customer2")
      stmt.execute(
        // language=PostgreSQL
        """
          CREATE TABLE sales.customer2 (
              customer_id INT NOT NULL
          ,   first_name TEXT NOT NULL
          ,   last_name TEXT NOT NULL
          ,   middle_initial TEXT NULL
          ,   date_added TIMESTAMP NOT NULL DEFAULT now()
          ,   date_updated TIMESTAMP NULL
          ,   kda_ts TIMESTAMPTZ(0) NULL
          )
          """
      )
    }
  }

  fun updateCustomer(customer: Customer) {
    // language=PostgreSQL
    val sql = """
    UPDATE sales.$tableName 
    SET
      first_name = ?
    , last_name = ?
    , middle_initial = ?
    , date_added = ?
    , date_updated = ?
    WHERE 
      customer_id = ?
  """
    con.prepareStatement(sql).use { stmt ->
      stmt.setString(1, customer.firstName)
      stmt.setString(2, customer.lastName)
      if (customer.middleInitial == null) {
        stmt.setNull(3, Types.VARCHAR)
      } else {
        stmt.setString(3, customer.middleInitial)
      }
      stmt.setTimestamp(4, Timestamp.valueOf(customer.dateAdded))
      if (customer.dateUpdated == null) {
        stmt.setNull(5, Types.TIMESTAMP)
      } else {
        stmt.setTimestamp(5, Timestamp.valueOf(customer.dateUpdated))
      }
      stmt.setInt(6, customer.customerId)
      stmt.execute()
    }
  }
}
