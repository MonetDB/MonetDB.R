library(testthat)
library(DBI)

test_that("we can start and stop a monetdbserver from R", {

    datadir <- "/tmp/unit-test_database"

    # clean dir beforehand
    unlink(datadir, T)

    # check if we can create a creation script
    script <- MonetDB.R::monetdb.server.setup(database.directory=datadir, dbname="db1", dbport=50001)
    expect_equal(script, '/tmp/unit-test_database/db1.sh')

    # check if the script got an integer assigned as PID.
    # that way we know that monetdb actually started.
    pid <- MonetDB.R::monetdb.server.start(script)
    expect_that(pid, is_a('integer'))

    # check if we can stop the server.
    MonetDB.R::monetdb.server.stop(pid)
    
    # clean up after.
    unlink(datadir, T)
})