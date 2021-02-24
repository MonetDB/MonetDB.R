library(DBI)

test_that("we can send query to pull results in batches", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbWriteTable(conn, "usarrests", datasets::USArrests, temporary = TRUE), T
      )
      res <- dbSendQuery(conn, "SELECT * FROM usarrests")
      blk <- dbFetch(res, n = 10)
      expect_equal(nrow(blk), 10)
      expect_equal(dbHasCompleted(res), F)
      blk <- dbFetch(res, n = 10)
      expect_equal(nrow(blk), 10)
      expect_equal(dbHasCompleted(res), F)
      # fetch a large number to empty `res`
      blk <- dbFetch(res, n = 10000)
      expect_equal(dbHasCompleted(res), T)
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "usarrests"), T)
    }
  )
})

test_that("we can fetch all results with n = -1", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbWriteTable(conn, "usarrests", datasets::USArrests, temporary = TRUE), T
      )
      res <- dbSendQuery(conn, "SELECT * FROM usarrests")
      expect_equal(nrow(dbFetch(res, n = -1)) > 0, T)
      expect_equal(dbHasCompleted(res), T)
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "usarrests"), T)
    }
  )
})

test_that("we can fetch all results with n = Inf", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbWriteTable(conn, "usarrests", datasets::USArrests, temporary = TRUE), T
      )
      res <- dbSendQuery(conn, "SELECT * FROM usarrests")
      expect_equal(nrow(dbFetch(res, n = Inf)) > 0, T)
      expect_equal(dbHasCompleted(res), T)
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "usarrests"), T)
    }
  )
})

test_that("we cannot fetch results from closed response", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbWriteTable(conn, "usarrests", datasets::USArrests, temporary = TRUE), T
      )
      res <- dbSendQuery(conn, "SELECT * FROM usarrests")
      blk <- dbFetch(res, n = 5)
      expect_equal(nrow(blk), 5)
      expect_equal(dbClearResult(res), T)
      expect_error(
        dbFetch(res, n = 5),
        "Cannot fetch results from closed response"
      )
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "usarrests"), T)
    }
  )
})
