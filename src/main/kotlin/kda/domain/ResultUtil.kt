package kda.domain

fun <T, R> Result<T>.flatMap(block: (T) -> (Result<R>)): Result<R> {
  return this.mapCatching {
    block(it).getOrThrow()
  }
}