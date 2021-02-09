#' MonetDB transaction management.
#'
#' Functions to begin, commit or rollback SQL transactions
#'
#' `dbBegin()` starts a transaction. `dbCommit()` and `dbRollback()`
#' end the transaction by either committing or rolling back the changes.
#'
#' @param conn a [MonetDBConnection-class] object, produced by
#'   [DBI::dbConnect()]
#' @param ... Unused, for extensibility.
#' @return A boolean, indicating success or failure.
#' @examples
#' # Only run the examples on systems with MonetDB connection:
#' run <- monetdbHasDefault()
#'
#' library(DBI)
#' if (run) con <- dbConnect(MonetDB.R(), "monetdb://localhost/demo")
#' if (run) dbWriteTable(con, "USarrests", datasets::USArrests, temporary = TRUE)
#' if (run) dbGetQuery(con, 'SELECT count(*) from "USarrests"')
#'
#' if (run) dbBegin(con)
#' if (run) dbExecute(con, 'DELETE from "USarrests" WHERE "Murder" > 1')
#' if (run) dbGetQuery(con, 'SELECT count(*) from "USarrests"')
#' if (run) dbRollback(con)
#'
#' # Rolling back changes leads to original count
#' if (run) dbGetQuery(con, 'SELECT count(*) from "USarrests"')
#'
#' if (run) dbRemoveTable(con, "USarrests")
#' if (run) dbDisconnect(con)
#' @include MonetDBConnection.R
#' @name monetdb-transactions
NULL

#' @export
#' @rdname monetdb-transactions
setMethod("dbBegin", "MonetDBConnection", function(conn, ...) {
  dbSendQuery(conn, "START TRANSACTION")
  invisible(TRUE)
})

#' @export
#' @rdname monetdb-transactions
setMethod("dbCommit", "MonetDBConnection", function(conn, ...) {
  dbSendQuery(conn, "COMMIT")
  invisible(TRUE)
})

#' @export
#' @rdname monetdb-transactions
setMethod("dbRollback", "MonetDBConnection", function(conn, ...) {
  dbSendQuery(conn, "ROLLBACK")
  invisible(TRUE)
})

# dbTransaction() #DEPRECATED
if (is.null(getGeneric("dbTransaction"))) {
  setGeneric(
    "dbTransaction",
    function(conn, ...) standardGeneric("dbTransaction")
  )
}
#' @name dbTransaction
#' @title Run a multi-statements transaction
#' @description
#' This method is DEPRECATED. Please use [monetdb-transactions] functions
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
#' # Only run the examples on systems with MonetDB connection:
#' run <- monetdbHasDefault()
#'
#' library(DBI)
#' if (run) conn <- dbConnect(MonetDB.R(), "monetdb://localhost/demo")
#' if (run) dbSendUpdate(conn, "CREATE TABLE foo(a INT,b VARCHAR(100))")
#' if (run) dbTransaction(conn)
#' if (run) dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 42, "bar")
#' if (run) dbCommit(conn)
#' if (run) dbTransaction(conn)
#' if (run) dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 43, "bar")
#' if (run) dbRollback(conn)
#' @export
setMethod("dbTransaction", signature(conn = "MonetDBConnection"),
  function(conn, ...) {
    dbBegin(conn)
    warning("dbTransaction() is deprecated, use dbBegin() from now.")
    invisible(TRUE)
  }
)
