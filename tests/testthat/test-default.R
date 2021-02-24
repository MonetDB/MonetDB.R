library(DBI)

test_that("we can find and connect to the default MonetDB database", {
  expect_equal(foundDefaultMonetDBdatabase(), T)
  conn <- monetdbDefault()
  expect_equal(dbIsValid(conn), TRUE)
  expect_that(conn, is_a("MonetDBConnection"))
  dbDisconnect(conn)
})
