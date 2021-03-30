library(DBI)

test_that("we can load a simple data set using the default parameters values", {
  conn <- monetdbDefault()
  on.exit(dbDisconnect(conn))

  file <- tempfile()
  res <- write.table(iris, file, sep = ",", row.names = FALSE)
  expect_equal(is.null(res), TRUE)
  res <- monetdb.read.csv(conn, file, "iris")
  expect_equal(is.null(res), TRUE)
  on.exit(dbRemoveTable(conn, "iris"), add = TRUE, after = FALSE)

  res <- DBI::dbGetQuery(
    conn,
    'SELECT COUNT(*), COUNT(DISTINCT "Species") FROM iris'
  )
  expect_equal(res$`%1`, nrow(iris))
  expect_equal(res$`%2`, length(unique(iris[, 5])))
})

test_that("we can load a simple data set specifying all parameters values", {
  conn <- monetdbDefault()
  on.exit(dbDisconnect(conn))

  file <- tempfile()
  res <- write.table(CO2, file, sep = ",", row.names = FALSE)
  expect_equal(is.null(res), TRUE)
  res <- monetdb.read.csv(conn, file, "CO2",
    header = TRUE, best.effort = FALSE,
    delim = ",", newline = "\\n", quote = "\"", create = TRUE,
    col.names = NULL, lower.case.names = FALSE, sep = ","
  )
  expect_equal(is.null(res), TRUE)
  on.exit(dbRemoveTable(conn, "CO2"), add = TRUE, after = FALSE)

  res <- DBI::dbGetQuery(
    conn,
    'SELECT COUNT(*) FROM "CO2"'
  )
  expect_equal(res$`%1`, nrow(CO2))
})
