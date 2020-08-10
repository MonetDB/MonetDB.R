library(DBI)

conn <- dbConnect(MonetDB.R::MonetDB())

# Check if we can start the database.
# This only works if the daemon and the database have been started.
test_that("db starts", {
    expect_equal(dbIsValid(conn), TRUE)
    expect_that(conn, is_a("MonetDBConnection"))    
})

 
# Checks if we can disconnect
# and the database no longer can be queried.
test_that("we can disconnect", {
    dbDisconnect(conn)

    # Check if we can no longer query the database.
    # sidenote: you can also use `dbReadTable` for this.
    expect_error(dbGetQuery(conn, "select tables.name from tables where system.tables=false;") , 'invalid connection') 
})
