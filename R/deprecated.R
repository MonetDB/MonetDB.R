#' @include MonetDBResult.R

#' @title Deprecated functions
#' @description
#' Deprecated functions are kept here for backward compatibility purpose.
#'
#' The function `fetch()` is a wrapper for [MonetDB.R::dbFetch()], which is
#' needed because DBI on CRAN still uses fetch()
#' @inheritParams DBI::dbFetch
#' @seealso [dbFetch()]
#' @export
#' @rdname deprecated
setMethod(
  "fetch", signature(res = "MonetDBResult", n = "numeric"),
  function(res, n, ...) {
    # .Deprecated("dbFetch")
    dbFetch(res, n, ...)
  }
)

# dbTransaction() #DEPRECATED
#' @title Run a multi-statements transaction
#' @description
#' This method is DEPRECATED. Please use [transactions] functions
#' instead.
#'
#' `dbTransaction()` is used to switch the query processing from the normal
#' auto-commiting mode into the transactional mode. Here, changes to the
#' database will not be permanent until [dbCommit()] is called. If the changes
#' are not to be persisted in the database, you can use [dbRollback()] to undo
#' all the changes since `dbTransaction()` was called.
#'
#' @param conn
#'        A MonetDB.R database connection, created using [DBI::dbConnect] with
#'        the [MonetDB.R] database driver.
#' @param ... More parameters. Ignored.
#' @return TRUE if the transaction was successful
#'
#' @examples
#' library(DBI)
#' if (foundDefaultMonetDBdatabase()) {
#'   conn <- dbConnect(MonetDB.R())
#'   dbSendUpdate(conn, "CREATE TABLE foo(a INT,b VARCHAR(100))")
#'   dbTransaction(conn)
#'   dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 42, "bar")
#'   dbCommit(conn)
#'   # we expect one record
#'   dbReadTable(conn, "foo")
#'
#'   dbTransaction(conn)
#'   dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 43, "bar")
#'   dbRollback(conn)
#'   # we still expect one record
#'   dbReadTable(conn, "foo")
#'   dbRemoveTable(conn, "foo")
#'   dbDisconnect(conn)
#' }
#' @export
#' @rdname dbTransaction
setGeneric(
  "dbTransaction",
  function(conn, ...) standardGeneric("dbTransaction")
)

#' @export
#' @rdname dbTransaction
setMethod(
  "dbTransaction", signature(conn = "MonetDBConnection"),
  function(conn, ...) {
    dbBegin(conn)
    warning("dbTransaction() is deprecated, use dbBegin() from now.")
    invisible(TRUE)
  }
)
