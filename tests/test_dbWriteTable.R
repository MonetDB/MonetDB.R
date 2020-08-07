library(DBI)
library(testthat)

conn <- dbConnect(MonetDB.R::MonetDB())

test_that("we can write a table to the database", {

    dbWriteTable(conn, "mtcars", mtcars[1:5,], overwrite=T)
    expect_equal(dbExistsTable(conn, "mtcars"), T)
})

test_that("special characters work", {
    table_name <- "specialcharsfoo"
    pi <- enc2utf8('U+03C0')

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

test_that("we can drop a table", {

    dbRemoveTable(conn, "mtcars")
    expect_equal(dbExistsTable(conn, "mtcars"), F)
    dbRemoveTable(conn, "foo")
    dbRemoveTable(conn, "foo1")
})


test_that("we can create a temporary table, and that its actually temporary", {
    table_name <- "fooTempTable"
    dbCreateTable(conn, table_name, iris, temporary=T)
    expect_equal(dbExistsTable(conn, table_name), T)

    dbDisconnect(conn)
    conn <- dbConnect(MonetDB.R::MonetDB())

    expect_equal(dbExistsTable(conn, table_name), F)
})