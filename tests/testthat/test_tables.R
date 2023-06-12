library(DBI)

test_that("we can list the tables in the database", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))
  dbListTables()
})

test_that("we can list the fields of a table", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))
  dbListFields()
})

test_that("we can read the contents of a table", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))
  dbReadTable()
})

test_that("we can write a table to the database with default parameters", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  dbWriteTable(conn, "mtcars", mtcars[1:5, ], overwrite = T)
  on.exit(dbRemoveTable(conn, "mtcars"), add = TRUE, after = FALSE)

  expect_equal(dbExistsTable(conn, "mtcars"), T)
})

test_that("we can write a table to the database with all parameters", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  dbWriteTable(conn, "mtcars", mtcars[1:5, ], overwrite = T)
  on.exit(dbRemoveTable(conn, "mtcars"), add = TRUE, after = FALSE)

  expect_equal(dbExistsTable(conn, "mtcars"), T)
})

test_that("we can check if a table exists", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))
  dbExistsTable()
})

test_that("we can remove a table", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))
  dbExistsTable()
})

test_that("special characters work", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  table_name <- "specialcharsfoo"
  pi <- enc2utf8("U+03C0")

  DBI::dbExecute(conn, paste0("CREATE TABLE ", table_name, "(x TEXT);"))
  on.exit(dbRemoveTable(conn, table_name), add = TRUE, after = FALSE)

  DBI::dbExecute(
    conn,
    paste0("INSERT INTO ", table_name, " VALUES ('U+03C0'); ")
  )

  expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM specialcharsfoo")$x, pi)
})

test_that("we can update a table", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  MonetDB.R::dbSendUpdate(conn, "CREATE TABLE foo(a INT, b INT)")
  on.exit(dbRemoveTable(conn, "foo"), add = TRUE, after = FALSE)

  expect_equal(dbExistsTable(conn, "foo"), T)
})

test_that("we can update a table async", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  MonetDB.R::dbSendUpdateAsync(conn, "CREATE TABLE foo(a INT, b INT)")
  on.exit(dbRemoveTable(conn, "foo"), add = TRUE, after = FALSE)

  expect_equal(dbExistsTable(conn, "foo"), T)
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
  # dbRemoveTable(conn, "foo")
  # dbRemoveTable(conn, "foo1")
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
    "Роман Mühleisen", DBI::dbGetQuery(conn, "SELECT a FROM monetdbtest")$a[[1]]
  )
  dbSendQuery(conn, "DELETE FROM monetdbtest")
  MonetDB.R::dbSendUpdate(
    conn, "INSERT INTO monetdbtest (a) VALUES (?)", "Роман Mühleisen"
  )
  expect_equal(
    "Роман Mühleisen", DBI::dbGetQuery(conn, "SELECT a FROM monetdbtest")$a[[1]]
  )
  dbRemoveTable(conn, tname)
})

test_that("we can create a temporary table, and that its actually temporary", {
  table_name <- "fooTempTable"
  DBI::dbCreateTable(conn, table_name, iris, temporary = T)
  expect_equal(dbExistsTable(conn, table_name), T)

  dbDisconnect(conn)
  conn <- dbConnect(MonetDB.R::MonetDB())

  expect_equal(dbExistsTable(conn, table_name), F)
})
