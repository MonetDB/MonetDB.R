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

test_that("we can commit a transaction", {

    dbBegin(conn)

    # we need to disable transaction here,
    # because it doesn't support nested transactions
    dbWriteTable(conn, "mtcars", mtcars[1:5,], transaction=F, overwrite=T)
    dbCommit(conn)

    output <- dbListTables(conn)
    expect_equal(grepl(output, "mtcars"), T)
    dbRemoveTable(conn, "mtcars")
})

test_that("we can rollback a transaction", {

    dbBegin(conn)

    dbWriteTable(conn, "mtcars", mtcars[1:5,], transaction=F, overwrite=T)
    dbRollback(conn)

    output <- dbListTables(conn)
    expect_error(grepl(str(output), "mtcars"))
})

# Checks if we can disconnect
# and the database no longer can be queried.
test_that("we can disconnect", {
    dbDisconnect(conn)

    # Check if we can query the database.
    # sidenote: you can also use `dbReadTable` for this.
    expect_error(dbGetQuery(conn, "select tables.name from tables where system.tables=false;") , 'invalid connection') 
})

# Check if we can shutdown a server 
# with the shutdown command, since server.stop() is actually deprecated.
test_that("we can shutdown a server", {

    conn <- dbConnect(MonetDB.R::MonetDB(), "demo")

    MonetDB.R::monetdb.server.shutdown(conn)
    expect_error(dbGetQuery(conn, "select 1"))
})