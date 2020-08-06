conn <- dbConnect(MonetDB.R::MonetDB())

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