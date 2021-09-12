package kda

fun standardizeSQL(sql: String) =
  sql
    .split("\n")
    .joinToString(" ")
    .replace("\\s+".toRegex(), " ")
    .replace("( ", "(")
    .replace(" )", ")")
    .trim()
