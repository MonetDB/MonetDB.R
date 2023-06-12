library(DBI)

test_that("we can commit a transaction", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      dbBegin(conn)
      # we need to disable transaction here,
      # because it doesn't support nested transactions
      dbWriteTable(conn, "mtcars", mtcars[1:5, ], transaction = F, overwrite = T)
      dbCommit(conn)

      expect_equal("mtcars" %in% dbListTables(conn), T)
    },
    finally = {
      dbRemoveTable(conn, "mtcars")
    }
  )
})

test_that("we can rollback a transaction", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  dbBegin(conn)
  dbWriteTable(conn, "mtcars", mtcars[1:5, ], transaction = F, overwrite = T)
  dbRollback(conn)

  expect_equal(dbExistsTable(conn, "mtcars"), F)
})

tsize <- function(conn, tname) {
  as.integer(DBI::dbGetQuery(conn, paste0("SELECT COUNT(*) FROM ", tname))[[1]])
}

test_that("transactions are ACID", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tname <- "monetdbtest"
  dbSendQuery(conn, paste("create table", tname, "(a integer)"))
  on.exit(dbRemoveTable(conn, tname), add = TRUE, after = FALSE)
  expect_true(dbExistsTable(conn, tname))

  dbBegin(conn)
  dbSendQuery(conn, paste("INSERT INTO", tname, "VALUES (42)"))
  expect_equal(tsize(conn, tname), 1)
  dbRollback(conn)
  expect_equal(tsize(conn, tname), 0)

  dbBegin(conn)
  dbSendQuery(conn, paste("INSERT INTO", tname, "VALUES (42)"))
  expect_equal(tsize(conn, tname), 1)
  dbCommit(conn)
  expect_equal(tsize(conn, tname), 1)
})
