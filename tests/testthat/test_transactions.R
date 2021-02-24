library(DBI)

test_that("we can commit a transaction", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  dbBegin(conn)
  # we need to disable transaction here,
  # because it doesn't support nested transactions
  dbWriteTable(conn, "mtcars", mtcars[1:5, ], transaction = F, overwrite = T)
  dbCommit(conn)

  expect_equal("mtcars" %in% dbListTables(conn), T)
  dbRemoveTable(conn, "mtcars")
})

test_that("we can rollback a transaction", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  dbBegin(conn)
  dbWriteTable(conn, "mtcars", mtcars[1:5, ], transaction = F, overwrite = T)
  dbRollback(conn)

  expect_equal(identical(dbListTables(conn), character(0)), T)
})
