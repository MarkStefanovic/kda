package kda.adapter

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kda.domain.DataTypeName
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.transactions.transaction

interface Db {
  fun exec(statement: Transaction.() -> Unit)

  fun <R> fetch(statement: Transaction.() -> R): R

  fun createTables()

  fun dropTables()
}

class SQLDb(private val ds: HikariDataSource) : Db {
  private val db: Database by lazy {
    Database.connect(ds)
  }

  override fun createTables() {
    transaction(db = db) {
//      addLogger(StdOutSqlLogger)
      SchemaUtils.create(PrimaryKeys, TableDefs, LatestTimestamps)
    }
  }

  override fun dropTables() {
    transaction(db = db) {
      SchemaUtils.drop(PrimaryKeys, TableDefs, LatestTimestamps)
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

fun sqliteHikariDatasource(
  dbPath: String = "./cache.db",
  driverClassName: String = "org.sqlite.JDBC"
): HikariDataSource {
  val config = HikariConfig()
  config.jdbcUrl = "jdbc:sqlite:$dbPath"
  config.driverClassName = driverClassName
  config.maximumPoolSize = 1
  config.transactionIsolation = "TRANSACTION_SERIALIZABLE"
  config.connectionTestQuery = "SELECT 1"
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  return HikariDataSource(config)
}

object TableDefs : Table("kda_table_def") {
  val schema = text("schema_name")
  val table = text("table_name")
  val column = text("column_name")
  val dataType = enumerationByName("data_type", 40, DataTypeName::class)
  val maxLength = integer("max_length").nullable()
  val precision = integer("precision").nullable()
  val scale = integer("scale").nullable()
  val autoincrement = bool("autoincrement").nullable()
  val dateAdded = datetime("date_added").defaultExpression(CurrentDateTime())

  override val primaryKey = PrimaryKey(schema, table, column, name = "pk_kda_table_def")
}

object PrimaryKeys : Table("kda_primary_key") {
  val schema = text("schema_name")
  val table = text("table_name")
  val fieldName = text("field_name")
  val order = integer("order")

  override val primaryKey = PrimaryKey(schema, table, order, name = "pk_kda_primary_key")
}

object LatestTimestamps : Table("kda_latest_timestamp") {
  val schema = text("schema_name")
  val table = text("table_name")
  val fieldName = text("field_name")
  val ts = datetime("ts").nullable()

  override val primaryKey = PrimaryKey(schema, table, fieldName, name = "pk_kda_latest_timestamp")
}
