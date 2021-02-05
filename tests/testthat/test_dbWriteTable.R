library(DBI)

conn <- dbConnect(MonetDB.R::MonetDB())

tsize <- function(conn, tname) {
  as.integer(dbGetQuery(conn, paste0("SELECT COUNT(*) FROM ", tname))[[1]])
}

test_that("we can write a table to the database", {
  dbWriteTable(conn, "mtcars", mtcars[1:5, ], overwrite = T)
  expect_equal(dbExistsTable(conn, "mtcars"), T)
})

test_that("the right keywords are used", {
  keywords <- dbGetQuery(conn, "SELECT * FROM sys.keywords")
  reserved_monetdb_keywords <<- sort(unique(array(c(unlist(keywords)))))
  all_keywords <- paste(
    "ADD, ADMIN, AFTER, AGGREGATE, ALL, ALTER, ALWAYS, ANALYZE, AND, ANY, ",
    "ASC, ASYMMETRIC, AT, ATOMIC, AUTHORIZATION, AUTO_INCREMENT, BEFORE, ",
    "BEGIN, BEST, BETWEEN, BIGINT, BIGSERIAL, BINARY, BLOB, BY, CACHE, CALL, ",
    "CASCADE, CASE, CAST, CENTURY, CHAIN, CHAR, CHARACTER, CHECK, CLIENT, ",
    "CLOB, COALESCE, COLUMN, COMMENT, COMMIT, COMMITTED, CONSTRAINT, ",
    "CONTINUE, CONVERT, COPY, CORRESPONDING, CREATE, CROSS, CUBE, CURRENT, ",
    "CURRENT_DATE, CURRENT_ROLE, CURRENT_TIME, CURRENT_TIMESTAMP, ",
    "CURRENT_USER, CYCLE, DATA, DATE, DAY, DEALLOCATE, DEBUG, DEC, DECADE, ",
    "DECIMAL, DECLARE, DEFAULT, DELETE, DELIMITERS, DESC, DIAGNOSTICS, ",
    "DISTINCT, DO, DOUBLE, DOW, DOY, DROP, EACH, EFFORT, ELSE, ELSEIF, ",
    "ENCRYPTED, END, ESCAPE, EVERY, EXCEPT, EXCLUDE, EXEC, EXECUTE, EXISTS, ",
    "EXPLAIN, EXTERNAL, EXTRACT, FALSE, FIRST, FLOAT, FOLLOWING, FOR, ",
    "FOREIGN, FROM, FULL, FUNCTION, FWF, GENERATED, GLOBAL, GRANT, GROUP, ",
    "GROUPING, GROUPS, HAVING, HOUR, HUGEINT, IDENTITY, IF, ILIKE, IN, ",
    "INCREMENT, INDEX, INNER, INSERT, INT, INTEGER, INTERSECT, INTERVAL, ",
    "INTO, IS, ISOLATION, JOIN, KEY, LANGUAGE, LARGE, LAST, LATERAL, LEFT, ",
    "LEVEL, LIKE, LIMIT, LOADER, LOCAL, LOCALTIME, LOCALTIMESTAMP, LOCKED, ",
    "MATCH, MATCHED, MAXVALUE, MEDIUMINT, MERGE, MINUTE, MINVALUE, MONTH, ",
    "NAME, NATURAL, NEW, NEXT, NO, NOT, NOW, NULLIF, NULLS, NUMERIC, OBJECT, ",
    "OF, OFFSET, OLD, ON, ONLY, OPTION, OPTIONS, OR, ORDER, OTHERS, OUTER, ",
    "OVER, PARTIAL, PARTITION, PASSWORD, PLAN, POSITION, PRECEDING, ",
    "PRECISION, PREP, PREPARE, PRESERVE, PRIMARY, PRIVILEGES, PROCEDURE, ",
    "PUBLIC, QUARTER, RANGE, READ, REAL, RECORDS, REFERENCES, REFERENCING, ",
    "RELEASE, REMOTE, RENAME, REPEATABLE, REPLACE, REPLICA, RESTART, ",
    "RESTRICT, RETURN, RETURNS, REVOKE, RIGHT, ROLLBACK, ROLLUP, ROWS, ",
    "SAMPLE, SAVEPOINT, SCHEMA, SECOND, SEED, SELECT, SEQUENCE, SERIAL, ",
    "SERIALIZABLE, SERVER, SESSION, SESSION_USER, SET, SETS, SIMPLE, SIZE, ",
    "SMALLINT, SOME, SPLIT_PART, START, STATEMENT, STDIN, STDOUT, STORAGE, ",
    "STREAM, STRING, SUBSTRING, SYMMETRIC, TABLE, TEMP, TEMPORARY, TEXT, ",
    "THEN, TIES, TIME, TIMESTAMP, TINYINT, TO, TRACE, TRANSACTION, TRIGGER, ",
    "TRUE, TRUNCATE, TYPE, UNBOUNDED, UNCOMMITTED, UNENCRYPTED, UNION, ",
    "UNIQUE, UPDATE, USER, USING, VALUES, VARCHAR, VARYING, VIEW, WEEK, WHEN, ",
    "WHERE, WHILE, WINDOW, WITH, WORK, WRITE, XMLAGG, XMLATTRIBUTES, ",
    "XMLCOMMENT, XMLCONCAT, XMLDOCUMENT, XMLELEMENT, XMLFOREST, ",
    "XMLNAMESPACES, XMLPARSE, XMLPI, XMLQUERY, XMLSCHEMA, XMLTEXT, ",
    "XMLVALIDATE, YEAR, ZONE"
  )

  expect_true(length(reserved_monetdb_keywords) > 0)
  expect_true(paste(reserved_monetdb_keywords, collapse = ", ") == all_keywords)
})

test_that("special characters work", {
  table_name <- "specialcharsfoo"
  pi <- enc2utf8("U+03C0")

  dbExecute(conn, paste0("CREATE TABLE ", table_name, "(x TEXT);"))
  dbExecute(conn, paste0("INSERT INTO ", table_name, " VALUES ('U+03C0'); "))

  expect_equal(dbGetQuery(conn, "SELECT * FROM specialcharsfoo")$x, pi)

  dbRemoveTable(conn, "specialcharsfoo")
})


test_that("we can update a table", {
  MonetDB.R::dbSendUpdate(conn, "CREATE TABLE foo(a INT, b INT)")
  expect_equal(dbExistsTable(conn, "foo"), T)
  dbRemoveTable(conn, "foo")
})

test_that("we can update a table async", {
  MonetDB.R::dbSendUpdateAsync(conn, "CREATE TABLE foo(a INT, b INT)")
  expect_equal(dbExistsTable(conn, "foo"), T)
  dbRemoveTable(conn, "foo")
})

test_that("transactions are on ACID", {
  tname <- "monetdbtest"
  dbSendQuery(conn, "create table monetdbtest (a integer)")
  expect_true(dbExistsTable(conn, tname))
  dbBegin(conn)
  dbSendQuery(conn, "INSERT INTO monetdbtest VALUES (42)")
  expect_equal(tsize(conn, tname), 1)
  dbRollback(conn)
  expect_equal(tsize(conn, tname), 0)
  dbBegin(conn)
  dbSendQuery(conn, "INSERT INTO monetdbtest VALUES (42)")
  expect_equal(tsize(conn, tname), 1)
  dbCommit(conn)
  expect_equal(tsize(conn, tname), 1)

  dbRemoveTable(conn, tname)
})

test_that("various parameters to dbWriteTable work as expected", {
  tname <- "monetdbtest"
  dbWriteTable(conn, tname, mtcars, append = F, overwrite = F)
  expect_true(dbExistsTable(conn, tname))

  expect_equal(tsize(conn, tname), nrow(mtcars))
  expect_error(dbWriteTable(conn, tname, mtcars, append = F, overwrite = F))
  expect_error(dbWriteTable(conn, tname, mtcars, overwrite = T, append = T))

  dbWriteTable(conn, tname, mtcars, append = F, overwrite = T)
  expect_true(dbExistsTable(conn, tname))
  expect_equal(tsize(conn, tname), nrow(mtcars))

  dbWriteTable(conn, tname, mtcars, append = T, overwrite = F)
  expect_equal(tsize(conn, tname), nrow(mtcars) * 2)

  dbRemoveTable(conn, tname)
  dbWriteTable(conn, tname, mtcars, append = F, overwrite = F, insert = T)
  expect_equal(tsize(conn, tname), nrow(mtcars))
  dbRemoveTable(conn, tname)
})

test_that("dbWriteTable with csvdump works as expected", {
  tname <- "monetdbtest"

  dbWriteTable(conn, tname, mtcars, overwrite = T, csvdump = T)
  expect_true(dbExistsTable(conn, tname))

  expect_equal(tsize(conn, tname), nrow(mtcars))

  dbRemoveTable(conn, tname)
})


test_that("we can drop a table", {
  dbRemoveTable(conn, "mtcars")
  expect_equal(dbExistsTable(conn, "mtcars"), F)
  dbRemoveTable(conn, "foo")
  dbRemoveTable(conn, "foo1")
})

test_that("csv import works", {
  tname <- "csvunittest"
  tf <- tempfile()
  write.table(iris, tf, sep = ",", row.names = FALSE)
  MonetDB.R::monetdb.read.csv(conn, tf, tname)
  expect_true(dbExistsTable(conn, tname))

  iris3 <- dbReadTable(conn, tname)
  expect_equal(dim(iris), dim(iris3))
  expect_equal(dbListFields(conn, tname), names(iris))

  dbRemoveTable(conn, tname)
  expect_false(dbExistsTable(conn, tname))
})

test_that("strings can have exotic characters", {
  tname <- "monetdbtest"
  skip_on_os("windows")
  dbSendQuery(conn, "create table monetdbtest (a string)")
  expect_true(dbExistsTable(conn, tname))
  dbSendQuery(conn, "INSERT INTO monetdbtest VALUES ('Роман Mühleisen')")
  expect_equal(
    "Роман Mühleisen", dbGetQuery(conn, "SELECT a FROM monetdbtest")$a[[1]])
  dbSendQuery(conn, "DELETE FROM monetdbtest")
  MonetDB.R::dbSendUpdate(
    conn, "INSERT INTO monetdbtest (a) VALUES (?)", "Роман Mühleisen")
  expect_equal(
    "Роман Mühleisen", dbGetQuery(conn, "SELECT a FROM monetdbtest")$a[[1]])
  dbRemoveTable(conn, tname)
})

test_that("we can create a temporary table, and that its actually temporary", {
  table_name <- "fooTempTable"
  dbCreateTable(conn, table_name, iris, temporary = T)
  expect_equal(dbExistsTable(conn, table_name), T)

  dbDisconnect(conn)
  conn <- dbConnect(MonetDB.R::MonetDB())

  expect_equal(dbExistsTable(conn, table_name), F)
})
