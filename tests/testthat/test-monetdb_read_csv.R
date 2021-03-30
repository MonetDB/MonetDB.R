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
    'SELECT COUNT(*), COUNT(DISTINCT "Type") FROM "CO2"'
  )
  expect_equal(res$`%1`, nrow(CO2))
  expect_equal(res$`%2`, length(unique(CO2[, 2])))
})

test_that("we can specify a different delimitor", {
  conn <- monetdbDefault()
  on.exit(dbDisconnect(conn))

  file <- tempfile()
  res <- write.table(CO2, file, sep = ";", row.names = FALSE)
  expect_equal(is.null(res), TRUE)
  res <- monetdb.read.csv(conn, file, "CO2", delim = ";")
  expect_equal(is.null(res), TRUE)
  on.exit(dbRemoveTable(conn, "CO2"), add = TRUE, after = FALSE)

  res <- DBI::dbGetQuery(
    conn,
    'SELECT COUNT(*), MAX(conc) FROM "CO2"'
  )
  expect_equal(res$`%1`, nrow(CO2))
  expect_equal(res$`%2`, max(CO2[, 4]))
})

test_that("we can specify a different separator than the delimitor", {
  conn <- monetdbDefault()
  on.exit(dbDisconnect(conn))

  file <- tempfile()
  res <- write.table(CO2, file, sep = ";", row.names = FALSE)
  expect_equal(is.null(res), TRUE)
  res <- monetdb.read.csv(conn, file, "CO2", delim = "|", sep = ";")
  expect_equal(is.null(res), TRUE)
  on.exit(dbRemoveTable(conn, "CO2"), add = TRUE, after = FALSE)

  res <- DBI::dbGetQuery(
    conn,
    'SELECT COUNT(*), MAX(conc) FROM "CO2"'
  )
  expect_equal(res$`%1`, nrow(CO2))
  expect_equal(res$`%2`, max(CO2[, 4]))
})

test_that("we can load data to an existing table, i.e. create = FALSE", {
  conn <- monetdbDefault()
  on.exit(dbDisconnect(conn))

  file <- tempfile()
  res <- write.table(CO2, file, sep = ",", row.names = FALSE)
  expect_equal(is.null(res), TRUE)
  res <- monetdb.read.csv(conn, file, "CO2")
  expect_equal(is.null(res), TRUE)
  on.exit(dbRemoveTable(conn, "CO2"), add = TRUE, after = FALSE)
  res <- monetdb.read.csv(conn, file, "CO2", create = FALSE)

  res <- DBI::dbGetQuery(
    conn,
    'SELECT COUNT(*), COUNT(DISTINCT "Type") FROM "CO2"'
  )
  expect_equal(res$`%1`, nrow(CO2) * 2)
  expect_equal(res$`%2`, length(unique(CO2[, 2])))
})

test_that("Convert all column names to lower case in the database works", {
  conn <- monetdbDefault()
  on.exit(dbDisconnect(conn))

  file <- tempfile()
  res <- write.table(CO2, file, sep = ",", row.names = FALSE)
  expect_equal(is.null(res), TRUE)
  res <- monetdb.read.csv(conn, file, "CO2", lower.case.names = FALSE)
  expect_equal(is.null(res), TRUE)
  on.exit(dbRemoveTable(conn, "CO2"), add = TRUE, after = FALSE)

  res <- DBI::dbGetQuery(
    conn,
    paste(
      "SELECT c.name FROM columns c, tables t",
      "WHERE c.table_id = t.id AND t.name = 'CO2';"
    )
  )

  expect_equal(res$name[1], "Plant")
  expect_equal(res$name[2], "Type")
  expect_equal(res$name[3], "Treatment")
  expect_equal(res$name[4], "conc")
  expect_equal(res$name[5], "uptake")
})

# FIXME 1: lower.case.names is currently ignored. So, the above test is not
#          really testing the conversion of column names to lower case.
#          When this parameter is implemented, this test should be updated
#          accordingly.
# FIXME 2: the same holds for col.names
# FIXME 3: we're not use a different data set for each test because some string
#          columns have the type "integer", while some integer columns are
#          exported as quotes strings in the CSV files. See also
# stackoverflow.com/questions/38529998/r-string-column-is-considered-as-integer

