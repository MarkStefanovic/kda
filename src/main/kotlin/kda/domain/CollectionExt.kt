package kda.domain

inline fun <C : Collection<*>> C.ifNotEmpty(defaultValue: (C) -> C): C =
  if (isEmpty()) {
    this
  } else {
    defaultValue(this)
  }

inline fun <C : Collection<*>> C.onNotEmpty(block: (C) -> Unit) {
  if (isNotEmpty()) {
    block(this)
  }
}
