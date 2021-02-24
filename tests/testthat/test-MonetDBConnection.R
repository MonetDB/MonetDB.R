library(DBI)

# Check if we can start the database.
# This only works if the daemon and the database have been started.
# FIXME: this test doesn't do what's said in the test documentation
test_that("db starts", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  expect_equal(dbIsValid(conn), TRUE)
  expect_that(conn, is_a("MonetDBConnection"))
})

test_that("we can connect with customised parameter values", {
  conn <- dbConnect(MonetDB.R::MonetDB(),
    dbname = "demo", user = "monetdb",
    password = "monetdb", host = "localhost", port = 50000L
  )
  on.exit(dbDisconnect(conn))

  expect_equal(dbExistsTable(conn, "tables"), TRUE)
})

test_that("we can connect with a URL", {
  conn <- dbConnect(MonetDB.R::MonetDB(), "monetdb://localhost:50000/demo")
  on.exit(dbDisconnect(conn))

  expect_equal(dbExistsTable(conn, "tables"), TRUE)
})

test_that("an unkonw connection parameter is ignored", {
  conn <- dbConnect(MonetDB.R::MonetDB(),
    dbname = "demo", user = "monetdb",
    password = "monetdb", host = "localhost", port = 50000L,
    foo = "bar"
  )
  on.exit(dbDisconnect(conn))

  expect_equal(dbExistsTable(conn, "tables"), TRUE)
})

test_that("querying closed connection throws an error", {
  conn <- dbConnect(MonetDB.R::MonetDB())

  expect_equal(dbExistsTable(conn, "tables"), TRUE)
  dbDisconnect(conn)
  expect_error(dbExistsTable(conn, "tables"), "invalid connection")
})

# This is only a sanity check if anything major in the MonetDB user interface
# has changed
test_that("the MonetDB reserved keywords are still what we are aware of", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  keywords <- list(
    "ADD", "ADMIN", "AFTER", "AGGREGATE", "ALL", "ALTER",
    "ALWAYS", "ANALYZE", "AND", "ANY", "ASC", "ASYMMETRIC", "AT",
    "ATOMIC", "AUTHORIZATION", "AUTO_INCREMENT", "BEFORE",
    "BEGIN", "BEST", "BETWEEN", "BIGINT", "BIGSERIAL", "BINARY",
    "BLOB", "BY", "CACHE", "CALL", "CASCADE", "CASE", "CAST",
    "CENTURY", "CHAIN", "CHAR", "CHARACTER", "CHECK", "CLIENT",
    "CLOB", "COALESCE", "COLUMN", "COMMENT", "COMMIT",
    "COMMITTED", "CONSTRAINT", "CONTINUE", "CONVERT", "COPY",
    "CORRESPONDING", "CREATE", "CROSS", "CUBE", "CURRENT",
    "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME",
    "CURRENT_TIMESTAMP", "CURRENT_USER", "CYCLE", "DATA", "DATE",
    "DAY", "DEALLOCATE", "DEBUG", "DEC", "DECADE", "DECIMAL",
    "DECLARE", "DEFAULT", "DELETE", "DELIMITERS", "DESC",
    "DIAGNOSTICS", "DISTINCT", "DO", "DOUBLE", "DOW", "DOY",
    "DROP", "EACH", "EFFORT", "ELSE", "ELSEIF", "ENCRYPTED",
    "END", "ESCAPE", "EVERY", "EXCEPT", "EXCLUDE", "EXEC",
    "EXECUTE", "EXISTS", "EXPLAIN", "EXTERNAL", "EXTRACT",
    "FALSE", "FIRST", "FLOAT", "FOLLOWING", "FOR", "FOREIGN",
    "FROM", "FULL", "FUNCTION", "FWF", "GENERATED", "GLOBAL",
    "GRANT", "GROUP", "GROUPING", "GROUPS", "HAVING", "HOUR",
    "HUGEINT", "IDENTITY", "IF", "ILIKE", "IN", "INCREMENT",
    "INDEX", "INNER", "INSERT", "INT", "INTEGER", "INTERSECT",
    "INTERVAL", "INTO", "IS", "ISOLATION", "JOIN", "KEY",
    "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEFT", "LEVEL",
    "LIKE", "LIMIT", "LOADER", "LOCAL", "LOCALTIME",
    "LOCALTIMESTAMP", "LOCKED", "MATCH", "MATCHED", "MAXVALUE",
    "MEDIUMINT", "MERGE", "MINUTE", "MINVALUE", "MONTH", "NAME",
    "NATURAL", "NEW", "NEXT", "NO", "NOT", "NOW", "NULLIF",
    "NULLS", "NUMERIC", "OBJECT", "OF", "OFFSET", "OLD", "ON",
    "ONLY", "OPTION", "OPTIONS", "OR", "ORDER", "OTHERS", "OUTER",
    "OVER", "PARTIAL", "PARTITION", "PASSWORD", "PLAN", "POSITION",
    "PRECEDING", "PRECISION", "PREP", "PREPARE", "PRESERVE",
    "PRIMARY", "PRIVILEGES", "PROCEDURE", "PUBLIC", "QUARTER",
    "RANGE", "READ", "REAL", "RECORDS", "REFERENCES",
    "REFERENCING", "RELEASE", "REMOTE", "RENAME", "REPEATABLE",
    "REPLACE", "REPLICA", "RESTART", "RESTRICT", "RETURN",
    "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROWS",
    "SAMPLE", "SAVEPOINT", "SCHEMA", "SECOND", "SEED", "SELECT",
    "SEQUENCE", "SERIAL", "SERIALIZABLE", "SERVER", "SESSION",
    "SESSION_USER", "SET", "SETS", "SIMPLE", "SIZE", "SMALLINT",
    "SOME", "SPLIT_PART", "START", "STATEMENT", "STDIN", "STDOUT",
    "STORAGE", "STREAM", "STRING", "SUBSTRING", "SYMMETRIC",
    "TABLE", "TEMP", "TEMPORARY", "TEXT", "THEN", "TIES", "TIME",
    "TIMESTAMP", "TINYINT", "TO", "TRACE", "TRANSACTION",
    "TRIGGER", "TRUE", "TRUNCATE", "TYPE", "UNBOUNDED",
    "UNCOMMITTED", "UNENCRYPTED", "UNION", "UNIQUE", "UPDATE",
    "USER", "USING", "VALUES", "VARCHAR", "VARYING", "VIEW",
    "WEEK", "WHEN", "WHERE", "WHILE", "WINDOW", "WITH", "WORK",
    "WRITE", "XMLAGG", "XMLATTRIBUTES", "XMLCOMMENT", "XMLCONCAT",
    "XMLDOCUMENT", "XMLELEMENT", "XMLFOREST", "XMLNAMESPACES",
    "XMLPARSE", "XMLPI", "XMLQUERY", "XMLSCHEMA", "XMLTEXT",
    "XMLVALIDATE", "YEAR", "ZONE"
  )

  expect_true(length(conn@connenv$keywords) > 0)
  expect_true(length(setdiff(keywords, conn@connenv$keywords)) == 0)
})
