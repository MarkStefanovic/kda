package kda.domain

interface LatestTimestampRepository {
  fun add(schema: String, table: String, latestTimestamp: LatestTimestamp)

  fun delete(schema: String, table: String)

  fun get(schema: String, table: String): Set<LatestTimestamp>
}
