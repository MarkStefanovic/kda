package kda.domain

sealed class DbAdapterError : Throwable() {
  object SerializableTransactionsNotSupported : DbAdapterError()
}
