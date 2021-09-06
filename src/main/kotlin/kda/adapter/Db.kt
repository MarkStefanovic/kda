package kda.adapter

import kda.domain.DataTypeName
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.`java-time`.CurrentDateTime
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.Connection

abstract class Db {
  abstract fun exec(statement: Transaction.() -> Unit)

  abstract fun <R> fetch(statement: Transaction.() -> R): R

  abstract fun createTables()
}

object SqliteDb : Db() {
  private val db by lazy {
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    Database.connect(url = "jdbc:sqlite:./cache.db", driver = "org.sqlite.JDBC")
  }

  override fun createTables() {
    transaction(db = db) {
//      addLogger(StdOutSqlLogger)
      SchemaUtils.create(PrimaryKeys, TableDefs)
    }
  }

  override fun exec(statement: Transaction.() -> Unit) {
    transaction(db = db) {
      statement()
    }
  }

  override fun <R> fetch(statement: Transaction.() -> R): R =
    transaction(db = db) {
      statement()
    }
}

object TableDefs : Table("table_def") {
  val schema = text("schema_name")
  val table = text("table_name")
  val column = text("column_name")
  val dataType = enumerationByName("data_type", 40, DataTypeName::class)
  val maxLength = integer("max_length").nullable()
  val precision = integer("precision").nullable()
  val scale = integer("scale").nullable()
  val autoincrement = bool("autoincrement").nullable()
  val dateAdded = datetime("date_added").defaultExpression(CurrentDateTime())

  override val primaryKey = PrimaryKey(schema, table, column, name = "pk_table_def")
}

object PrimaryKeys : Table("pkey") {
  val schema = text("schema_name")
  val table = text("table_name")
  val fieldName = text("field_name")
  val order = integer("order")

  override val primaryKey = PrimaryKey(schema, table, order, name = "pk_pkey")
}

object LatestTimestamps : Table("latest_timestamp") {
  val schema = text("schema_name")
  val table = text("table_name")
  val fieldName = text("field_name")
  val ts = datetime("ts")

  override val primaryKey = PrimaryKey(schema, table, fieldName, name = "pk_latest_timestamp")
}
