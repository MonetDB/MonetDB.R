library(DBI)

conn <- dbConnect(MonetDB.R::MonetDB())

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
