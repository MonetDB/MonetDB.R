library(DBI)

test_that("dbSendUpdate() works", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbSendUpdate(conn, "CREATE TABLE tbl(a INT,b VARCHAR(100))"), T
      )
      expect_equal(
        dbSendUpdate(conn, "INSERT INTO tbl VALUES(?,?)", 42, "bar"), T
      )
      expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM tbl")$a, 42)
      expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM tbl")$b, "bar")
    },
    finally = {
      expect_equal(dbSendUpdate(conn, "DROP TABLE tbl"), T)
    }
  )
})

test_that("dbSendUpdateAsync() works", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbSendUpdateAsync(conn, "CREATE TABLE tbl(a INT,b VARCHAR(100))"), T
      )
      expect_equal(
        dbSendUpdateAsync(conn, "INSERT INTO tbl VALUES(?,?)", 42, "bar"), T
      )
      expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM tbl")$a, 42)
      expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM tbl")$b, "bar")
    },
    finally = {
      expect_equal(dbSendUpdateAsync(conn, "DROP TABLE tbl"), T)
    }
  )
})
