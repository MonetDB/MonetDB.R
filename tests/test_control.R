library(testthat)
library(DBI)

test_that("we can start and stop a monetdbserver from R", {
    # check if we can create a creation script
    script <- MonetDB.R::monetdb.server.setup(database.directory="/tmp/database", dbname="db1", dbport=50001)


    # check if the script got an integer assigned as PID.
    # that way we know that monetdb actually started.
    pid <- MonetDB.R::monetdb.server.start(script)
    expect_that(pid, is_a('integer'))

    # check if we can stop the server.
    MonetDB.R::monetdb.server.stop(pid)
})