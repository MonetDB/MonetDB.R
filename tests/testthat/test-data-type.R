library(DBI)

# Taken from DBI
test_that("dbDataType works on a data frame", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  df <- data.frame(x = 1:10, y = 1:10 / 2)
  types <- dbDataType(conn, df)

  # MonetDB.R returns "STRING" for anything other than "BOOLEAN", "INTEGER",
  # "DOUBLE PRECISION" and "BLOB"
  # expect_equal(types, c(x = "INTEGER", y = "DOUBLE PRECISION"))
  expect_equal(types, "STRING")
})

test_that("dbColumnInfo() can handle MonetDB column types", {
  conn <- dbConnect(MonetDB.R::MonetDB())
  on.exit(dbDisconnect(conn))

  rs <- dbSendQuery(conn, paste(
    "SELECT ",
    "  CAST(42 AS TINYINT), ",
    "  CAST(43 AS SMALLINT),",
    "  CAST(44 AS INT),",
    "  CAST(45 AS BIGINT),",
    "  CAST(42.43 AS DECIMAL(8,2)),",
    "  CAST(42.42 AS FLOAT),",
    "  CAST(42.43 AS DOUBLE),",
    "  CAST('foo' AS CHAR),",
    "  CAST('foo' AS VARCHAR(5)),",
    "  CAST('foo' AS CLOB),",
    "  DATE '2000-01-01',",
    "  UUID '123e4567-e89b-12d3-a456-426614174000',",
    "  JSON '[1,2,3]',",
    "  BLOB 'aabbccdd1144'"
  ))

  expect_equal(
    dbColumnInfo(rs)[["field.type"]],
    c(
      "TINYINT", "SMALLINT", "INT",
      "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE",
      "CHAR", "VARCHAR", "CLOB", "DATE",
      "UUID", "JSON", "BLOB"
    )
  )
  expect_equal(
    dbColumnInfo(rs)[["field.type"]],
    dbColumnInfo(rs)[["monetdb.data.type"]]
  )

  expect_equal(
    dbColumnInfo(rs)[["data.type"]],
    c(
      "integer", "integer", "integer",
      "numeric", "numeric", "numeric", "numeric",
      "character", "character", "character", "character",
      "character", "character", "raw"
    )
  )
  expect_equal(
    dbColumnInfo(rs)[["data.type"]],
    dbColumnInfo(rs)[["r.data.type"]]
  )
})
