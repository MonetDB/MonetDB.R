test_that("we can query some metadata of a MonetDBResult object", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbWriteTable(conn, "usarrests", datasets::USArrests, temporary = TRUE), T
      )
      qry <- "SELECT * FROM usarrests"
      res <- dbSendQuery(conn, qry)
      expect_equal(dbGetRowCount(res), nrow(datasets::USArrests))
      expect_equal(dbGetStatement(res), qry)
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "usarrests"), T)
    }
  )
})
