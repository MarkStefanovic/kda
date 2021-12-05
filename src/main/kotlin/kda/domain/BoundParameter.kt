package kda.domain

data class BoundParameter(val parameter: Parameter, val value: Any?) {
  override fun toString(): String =
    "BoundParameter [ parameter: $parameter, value: $value ]"
}
