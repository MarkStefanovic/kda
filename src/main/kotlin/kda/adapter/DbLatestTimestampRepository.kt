package kda.adapter

import kda.domain.LatestTimestamp
import kda.domain.LatestTimestampRepository
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select

class DbLatestTimestampRepository(private val db: Db) : LatestTimestampRepository {
  override fun add(schema: String, table: String, latestTimestamp: LatestTimestamp) {
    db.exec {
      LatestTimestamps.insert {
        it[LatestTimestamps.schema] = schema
        it[LatestTimestamps.table] = table
        it[LatestTimestamps.fieldName] = latestTimestamp.fieldName
        it[LatestTimestamps.ts] = latestTimestamp.timestamp
      }
    }
  }

  override fun delete(schema: String, table: String) {
    db.exec {
      LatestTimestamps.deleteWhere {
        (LatestTimestamps.schema eq schema) and (LatestTimestamps.table eq table)
      }
    }
  }

  override fun get(schema: String, table: String): Set<LatestTimestamp> =
    db.fetch {
      LatestTimestamps.select {
        (LatestTimestamps.schema eq schema) and (LatestTimestamps.table eq table)
      }
        .map { row ->
          LatestTimestamp(
            fieldName = row[LatestTimestamps.fieldName],
            timestamp = row[LatestTimestamps.ts],
          )
        }
        .toSet()
    }
}
