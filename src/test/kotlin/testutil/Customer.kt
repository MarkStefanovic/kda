package testutil

import java.time.LocalDateTime

data class Customer(
  val customerId: Int,
  val firstName: String,
  val lastName: String,
  val middleInitial: String?,
  val dateAdded: LocalDateTime,
  val dateUpdated: LocalDateTime?,
)
