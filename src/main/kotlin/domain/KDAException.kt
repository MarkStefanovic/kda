package domain

abstract class KDAException(message: String) : Exception(message)

data class NoRowsReturned(val sql: String) :
    KDAException("The following query returned no results: $sql")

data class NotABool(val value: Any) : KDAException("$value is not a Bool.")

data class NotADate(val value: Any) : KDAException("$value is not a Date.")

data class NotADecimal(val value: Any) : KDAException("$value is not a Decimal.")

data class NotAFloat(val value: Any) : KDAException("$value is not a Float.")

data class NotAnInt(val value: Any) : KDAException("$value is not an Int.")

// data class NotAScalar(val sql: String) :
//    KDAException("Expecting a scalar result, but the following query returned multiple columns:
// $sql")

data class NotAString(val value: Any) : KDAException("$value is not a String.")

data class NotATimestamp(val value: Any) : KDAException("$value is not a Timestamp.")
