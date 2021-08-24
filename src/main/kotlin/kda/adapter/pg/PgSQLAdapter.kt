package kda.adapter.pg

import kda.adapter.std.StdSQLAdapter
import kda.adapter.std.StdSQLAdapterImplDetails
import kda.domain.SQLAdapter

val keywords =
  setOf(
    "FALSE",
    "TRUE",
    "A",
    "ABS",
    "ABSENT",
    "ACCORDING",
    "ACOS",
    "ADA",
    "ALL",
    "ALLOCATE",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ARE",
    "ARRAY",
    "ARRAY_AGG",
    "ARRAY_MAX_CARDINALITY",
    "AS",
    "ASC",
    "ASENSITIVE",
    "ASIN",
    "ASYMMETRIC",
    "ATAN",
    "ATOMIC",
    "ATTRIBUTES",
    "AUTHORIZATION",
    "AVG",
    "BASE64",
    "BEGIN_FRAME",
    "BEGIN_PARTITION",
    "BERNOULLI",
    "BINARY",
    "BIT_LENGTH",
    "BLOB",
    "BLOCKED",
    "BOM",
    "BOTH",
    "BREADTH",
    "C",
    "CARDINALITY",
    "CASE",
    "CAST",
    "CATALOG_NAME",
    "CEIL",
    "CEILING",
    "CHAINING",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "CHARACTER_SET_CATALOG",
    "CHARACTER_SET_NAME",
    "CHARACTER_SET_SCHEMA",
    "CHARACTERS",
    "CHECK",
    "CLASS_ORIGIN",
    "CLASSIFIER",
    "CLOB",
    "COBOL",
    "COLLATE",
    "COLLATION",
    "COLLATION_CATALOG",
    "COLLATION_NAME",
    "COLLATION_SCHEMA",
    "COLLECT",
    "COLUMN",
    "COLUMN_NAME",
    "COMMAND_FUNCTION",
    "COMMAND_FUNCTION_CODE",
    "CONCURRENTLY",
    "CONDITION",
    "CONDITION_NUMBER",
    "CONDITIONAL",
    "CONNECT",
    "CONNECTION_NAME",
    "CONSTRAINT",
    "CONSTRAINT_CATALOG",
    "CONSTRAINT_NAME",
    "CONSTRAINT_SCHEMA",
    "CONSTRUCTOR",
    "CONTAINS",
    "CONTROL",
    "CONVERT",
    "CORR",
    "CORRESPONDING",
    "COS",
    "COSH",
    "COUNT",
    "COVAR_POP",
    "COVAR_SAMP",
    "CREATE",
    "CROSS",
    "CUME_DIST",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_DEFAULT_TRANSFORM_GROUP",
    "CURRENT_PATH",
    "CURRENT_ROLE",
    "CURRENT_ROW",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
    "CURRENT_USER",
    "CURSOR_NAME",
    "DATALINK",
    "DATE",
    "DATETIME_INTERVAL_CODE",
    "DATETIME_INTERVAL_PRECISION",
    "DB",
    "DECFLOAT",
    "DEFAULT",
    "DEFERRABLE",
    "DEFINE",
    "DEFINED",
    "DEGREE",
    "DENSE_RANK",
    "DEPTH",
    "DEREF",
    "DERIVED",
    "DESC",
    "DESCRIBE",
    "DESCRIPTOR",
    "DETERMINISTIC",
    "DIAGNOSTICS",
    "DISCONNECT",
    "DISPATCH",
    "DISTINCT",
    "DLNEWCOPY",
    "DLPREVIOUSCOPY",
    "DLURLCOMPLETE",
    "DLURLCOMPLETEONLY",
    "DLURLCOMPLETEWRITE",
    "DLURLPATH",
    "DLURLPATHONLY",
    "DLURLPATHWRITE",
    "DLURLSCHEME",
    "DLURLSERVER",
    "DLVALUE",
    "DO",
    "DYNAMIC",
    "DYNAMIC_FUNCTION",
    "DYNAMIC_FUNCTION_CODE",
    "ELEMENT",
    "ELSE",
    "EMPTY",
    "END",
    "END_FRAME",
    "END_PARTITION",
    "END-EXEC",
    "ENFORCED",
    "EQUALS",
    "ERROR",
    "EVERY",
    "EXCEPT",
    "EXCEPTION",
    "EXEC",
    "EXP",
    "FETCH",
    "FILE",
    "FINAL",
    "FINISH",
    "FIRST_VALUE",
    "FLAG",
    "FLOOR",
    "FOR",
    "FOREIGN",
    "FORMAT",
    "FORTRAN",
    "FOUND",
    "FRAME_ROW",
    "FREE",
    "FREEZE",
    "FROM",
    "FS",
    "FULFILL",
    "FULL",
    "FUSION",
    "G",
    "GENERAL",
    "GET",
    "GO",
    "GOTO",
    "GRANT",
    "GROUP",
    "HAVING",
    "HEX",
    "HIERARCHY",
    "ID",
    "IGNORE",
    "ILIKE",
    "IMMEDIATELY",
    "IMPLEMENTATION",
    "IN",
    "INDENT",
    "INDICATOR",
    "INITIAL",
    "INITIALLY",
    "INNER",
    "INSTANCE",
    "INSTANTIABLE",
    "INTEGRITY",
    "INTERSECT",
    "INTERSECTION",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "JSON",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_EXISTS",
    "JSON_OBJECT",
    "JSON_OBJECTAGG",
    "JSON_QUERY",
    "JSON_TABLE",
    "JSON_TABLE_PRIMITIVE",
    "JSON_VALUE",
    "K",
    "KEEP",
    "KEY_MEMBER",
    "KEY_TYPE",
    "KEYS",
    "LAG",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEFT",
    "LENGTH",
    "LIBRARY",
    "LIKE",
    "LIKE_REGEX",
    "LIMIT",
    "LINK",
    "LISTAGG",
    "LN",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCATOR",
    "LOG",
    "LOG10",
    "LOWER",
    "M",
    "MAP",
    "MATCH_NUMBER",
    "MATCH_RECOGNIZE",
    "MATCHED",
    "MATCHES",
    "MAX",
    "MEASURES",
    "MEMBER",
    "MERGE",
    "MESSAGE_LENGTH",
    "MESSAGE_OCTET_LENGTH",
    "MESSAGE_TEXT",
    "MIN",
    "MOD",
    "MODIFIES",
    "MODULE",
    "MORE",
    "MULTISET",
    "MUMPS",
    "NAMESPACE",
    "NATURAL",
    "NCLOB",
    "NESTED",
    "NESTING",
    "NIL",
    "NOT",
    "NOTNULL",
    "NTH_VALUE",
    "NTILE",
    "NULL",
    "NULLABLE",
    "NUMBER",
    "OCCURRENCES_REGEX",
    "OCTET_LENGTH",
    "OCTETS",
    "OFFSET",
    "OMIT",
    "ON",
    "ONE",
    "ONLY",
    "OPEN",
    "OR",
    "ORDER",
    "ORDERING",
    "OUTER",
    "OUTPUT",
    "OVERFLOW",
    "OVERLAPS",
    "P",
    "PAD",
    "PARAMETER",
    "PARAMETER_MODE",
    "PARAMETER_NAME",
    "PARAMETER_ORDINAL_POSITION",
    "PARAMETER_SPECIFIC_CATALOG",
    "PARAMETER_SPECIFIC_NAME",
    "PARAMETER_SPECIFIC_SCHEMA",
    "PASCAL",
    "PASS",
    "PASSTHROUGH",
    "PAST",
    "PATH",
    "PATTERN",
    "PER",
    "PERCENT",
    "PERCENT_RANK",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "PERIOD",
    "PERMISSION",
    "PERMUTE",
    "PLACING",
    "PLAN",
    "PLI",
    "PORTION",
    "POSITION_REGEX",
    "POWER",
    "PRECEDES",
    "PRIMARY",
    "PRIVATE",
    "PRUNE",
    "PTF",
    "PUBLIC",
    "QUOTES",
    "RANK",
    "READS",
    "RECOVERY",
    "REFERENCES",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_COUNT",
    "REGR_INTERCEPT",
    "REGR_R2",
    "REGR_SLOPE",
    "REGR_SXX",
    "REGR_SXY",
    "REGR_SYY",
    "REQUIRING",
    "RESPECT",
    "RESTORE",
    "RESULT",
    "RETURN",
    "RETURNED_CARDINALITY",
    "RETURNED_LENGTH",
    "RETURNED_OCTET_LENGTH",
    "RETURNED_SQLSTATE",
    "RETURNING",
    "RIGHT",
    "ROUTINE_CATALOG",
    "ROUTINE_NAME",
    "ROUTINE_SCHEMA",
    "ROW_COUNT",
    "ROW_NUMBER",
    "RUNNING",
    "SCALAR",
    "SCALE",
    "SCHEMA_NAME",
    "SCOPE",
    "SCOPE_CATALOG",
    "SCOPE_NAME",
    "SCOPE_SCHEMA",
    "SECTION",
    "SEEK",
    "SELECT",
    "SELECTIVE",
    "SELF",
    "SENSITIVE",
    "SERVER_NAME",
    "SESSION_USER",
    "SIMILAR",
    "SIN",
    "SINH",
    "SIZE",
    "SOME",
    "SOURCE",
    "SPACE",
    "SPECIFIC",
    "SPECIFIC_NAME",
    "SPECIFICTYPE",
    "SQLCODE",
    "SQLERROR",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQRT",
    "STATE",
    "STATIC",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "STRING",
    "STRUCTURE",
    "STYLE",
    "SUBCLASS_ORIGIN",
    "SUBMULTISET",
    "SUBSET",
    "SUBSTRING_REGEX",
    "SUCCEEDS",
    "SUM",
    "SYMMETRIC",
    "SYSTEM_TIME",
    "SYSTEM_USER",
    "T",
    "TABLE",
    "TABLE_NAME",
    "TABLESAMPLE",
    "TAN",
    "TANH",
    "THEN",
    "THROUGH",
    "TIMEZONE_HOUR",
    "TIMEZONE_MINUTE",
    "TO",
    "TOKEN",
    "TOP_LEVEL_COUNT",
    "TRAILING",
    "TRANSACTION_ACTIVE",
    "TRANSACTIONS_COMMITTED",
    "TRANSACTIONS_ROLLED_BACK",
    "TRANSFORMS",
    "TRANSLATE",
    "TRANSLATE_REGEX",
    "TRANSLATION",
    "TRIGGER_CATALOG",
    "TRIGGER_NAME",
    "TRIGGER_SCHEMA",
    "TRIM_ARRAY",
    "UNCONDITIONAL",
    "UNDER",
    "UNION",
    "UNIQUE",
    "UNLINK",
    "UNMATCHED",
    "UNNAMED",
    "UNNEST",
    "UNTYPED",
    "UPPER",
    "URI",
    "USAGE",
    "USER",
    "USER_DEFINED_TYPE_CATALOG",
    "USER_DEFINED_TYPE_CODE",
    "USER_DEFINED_TYPE_NAME",
    "USER_DEFINED_TYPE_SCHEMA",
    "USING",
    "UTF16",
    "UTF32",
    "UTF8",
    "VALUE_OF",
    "VAR_POP",
    "VAR_SAMP",
    "VARBINARY",
    "VARIADIC",
    "VERBOSE",
    "VERSIONING",
    "WHEN",
    "WHENEVER",
    "WHERE",
    "WIDTH_BUCKET",
    "WINDOW",
    "WITH",
    "XMLAGG",
    "XMLBINARY",
    "XMLCAST",
    "XMLCOMMENT",
    "XMLDECLARATION",
    "XMLDOCUMENT",
    "XMLITERATE",
    "XMLQUERY",
    "XMLSCHEMA",
    "XMLTEXT",
    "XMLVALIDATE",
  )

class PgSQLAdapter(private val stdImpl: SQLAdapter) : SQLAdapter by stdImpl

val pgSQLAdapter =
  PgSQLAdapter(StdSQLAdapter(PgSQLAdapterImplDetails(StdSQLAdapterImplDetails(keywords))))
