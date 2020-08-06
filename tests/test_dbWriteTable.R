library(DBI)
library(testthat)

conn <- dbConnect(MonetDB.R::MonetDB())

test_that("we can write a table to the database", {

    # assert we can write a table to the database.
    dbWriteTable(conn, "mtcars", mtcars[1:5,], overwrite=T)
    expect_equal(dbExistsTable(conn, "mtcars"), T)
})

test_that("we can update a table", {
    MonetDB.R::dbSendUpdate(conn, "CREATE TABLE foo(a INT, b INT)")
    expect_equal(dbExistsTable(conn, "foo"), T)
})

test_that("we can update a table async", {
    MonetDB.R::dbSendUpdate(conn, "CREATE TABLE foo1(a INT, b INT)")
    expect_equal(dbExistsTable(conn, "foo"), T)
})

test_that("we can drop a table", {

    # assert we can drop a table
    dbRemoveTable(conn, "mtcars")
    expect_equal(dbExistsTable(conn, "mtcars"), F)
    dbRemoveTable(conn, "foo")
    dbRemoveTable(conn, "foo1")
})