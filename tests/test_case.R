library(testthat)
library(DBI)

conn <- dbConnect(MonetDB.R::MonetDB(), "demo")

# Check if we can start the database.
# This only works if the daemon and the database have been started.
test_that("db starts", {
    expect_equal(dbIsValid(conn), TRUE)
    expect_that(conn, is_a("MonetDBConnection"))    
})

# Checks if we can create a table,
# list the tables and drop tables.
test_that("we can manipulate tables in the database", {

    # assert we can write a table to the database.
    dbWriteTable(conn, "mtcars", mtcars[1:5,], overwrite=T)
    output <- dbListTables(conn) # test dbList aswell
    expect_equal(grepl(output, "mtcars"), T)

    # assert we can drop a table
    dbRemoveTable(conn, "mtcars")
    output <- dbListTables(conn) 
    expect_error(grepl(output, "mtcars"))

})

# Checks if we can disconnect
# and the database no longer can be queried.
test_that("we can disconnect", {
    dbDisconnect(conn)

    # Check if we can query the database.
    expect_error(dbGetQuery(conn, "select tables.name from tables where system.tables=false;") , 'invalid connection') 
})


skip("There is still a bug with the auto-commit.")
test_that("we can rollback a transaction", {

    dbWriteTable(conn, "mtcars", mtcars[1:5,])
    dbRollback(conn)

})