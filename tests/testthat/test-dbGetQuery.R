library(DBI)

test_that("we can run queries to get results as dataframe", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      expect_equal(
        dbWriteTable(conn, "usarrests", datasets::USArrests, temporary = TRUE), T
      )
      expect_equal(
        nrow(DBI::dbGetQuery(conn, "SELECT * FROM usarrests LIMIT 3")),
        3
      )
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "usarrests"), T)
    }
  )
})

test_that("special characters work", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      val <- enc2utf8("\u00e5")
      dbSendUpdate(conn, "CREATE TABLE tbl (x TEXT)")
      dbSendUpdate(conn, "INSERT INTO tbl VALUES ('\\u00e5')")
      expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM tbl")$x, val)
      expect_equal(
        DBI::dbGetQuery(conn, "SELECT * FROM tbl WHERE x = '\\u00e5'")$x, val
      )
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "tbl"), T)
    }
  )
})

test_that("UUID data works", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      uuid <- "123e4567-e89b-12d3-a456-426614174000"
      dbSendUpdate(conn, "CREATE TABLE tbl (id UUID, txt TEXT)")
      dbSendUpdate(conn, paste0("INSERT INTO tbl VALUES ('", uuid, "', 'foo')"))
      expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM tbl")$id, uuid)
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "tbl"), T)
    }
  )
})

test_that("JSON data works", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  tryCatch(
    {
      jsn <- "{\"val\": 42}"
      dbSendUpdate(conn, "CREATE TABLE tbl (jsn JSON)")
      dbSendUpdate(conn, paste0("INSERT INTO tbl VALUES ('", jsn, "')"))
      expect_equal(
        DBI::dbGetQuery(conn, "SELECT json.number(jsn) as v FROM tbl")$v, 42
      )
    },
    finally = {
      expect_equal(dbRemoveTable(conn, "tbl"), T)
    }
  )
})
