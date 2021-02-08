#' @name transactions
#' @title Begin/commit/rollback SQL transactions
#' @description
#' Transaction management.
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
NULL

####include MonetDBConnection.R

#' @export
#' @rdname transactions
setMethod("dbBegin", "MonetDBConnection", function(conn, ...) {
  dbSendQuery(conn, "START TRANSACTION")
  invisible(TRUE)
})

#' @export
#' @rdname transactions
setMethod("dbCommit", "MonetDBConnection", function(conn, ...) {
  dbSendQuery(conn, "COMMIT")
  invisible(TRUE)
})

#' @export
#' @rdname transactions
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
#' This method is DEPRECATED. Please use MonetDB's [transactions] functions
#' instead.
#'
#' `dbTransaction()` is used to switch the data from the normal
#' auto-commiting mode into transactional mode. Here, changes to the database
#' will not be permanent until [dbCommit()] is called. If the changes are
#' not to be kept around, you can use [dbRollback()] to undo all the changes
#' since `dbTransaction` was called.
#'
#' @param conn
#'        A MonetDB.R database connection, created using
#'        \code{\link[DBI]{dbConnect}} with the
#'        \code{\link[MonetDB.R]{MonetDB.R}} database driver.
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
#' @rdname dbTransaction
setMethod(
  "dbTransaction", signature(conn = "MonetDBConnection"),
  function(conn, ...) {
    dbBegin(conn)
    warning("dbTransaction() is deprecated, use dbBegin() from now.")
    invisible(TRUE)
  }
)
