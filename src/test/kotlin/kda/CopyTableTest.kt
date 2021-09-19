package kda

import kda.domain.CopyTableResult
import kda.domain.Dialect
import kda.shared.tableExists
import kda.shared.testPgConnection
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertIs

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CopyTableTest {
  @Test
  fun when_dest_does_not_exist_then_it_should_be_created() {
    testPgConnection().use { srcCon: Connection ->
      testPgConnection().use { destCon: Connection ->
        destCon.createStatement().use { stmt ->
          stmt.execute("DROP TABLE IF EXISTS sales.customer")
          stmt.execute("DROP TABLE IF EXISTS sales.customer2")
          stmt.execute(
            """
            CREATE TABLE sales.customer (
                customer_id SERIAL PRIMARY KEY
            ,   first_name TEXT
            ,   last_name TEXT
            )
            """
          )
        }
        assert(!tableExists(destCon, "sales", "customer2"))

        val result =
          copyTable(
            srcCon = srcCon,
            destCon = destCon,
            srcDialect = Dialect.PostgreSQL,
            destDialect = Dialect.PostgreSQL,
            srcSchema = "sales",
            srcTable = "customer",
            destSchema = "sales",
            destTable = "customer2",
            includeFields = setOf("customer_id", "first_name", "last_name"),
            primaryKeyFields = listOf("customer_id"),
          ).getOrThrow()
        assertIs<CopyTableResult>(result)
        assertEquals(
          expected = setOf("customer_id", "first_name", "last_name"),
          actual = result.srcTableDef.fields.map { fld -> fld.name }.toSet()
        )
        assert(tableExists(destCon, "sales", "customer2"))
      }
    }
  }
}
