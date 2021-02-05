#' @include MonetDBConnection.R

### dbTransaction() and friends ###
#' @name transactions
#' @title Begin/commit/rollback SQL transactions
#' @description
#' `dbBegin()` starts a transaction. `dbCommit()` and `dbRollback()` end the
#' transaction by either committing or rolling back the changes.
#'
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

if (is.null(getGeneric("dbTransaction"))) {
  setGeneric(
    "dbTransaction",
    function(conn, ...) standardGeneric("dbTransaction")
  )
}
#' @name transactions
#' @title Begin/commit/rollback SQL transactions
#' @description
#' \code{dbTransaction} is used to switch the data from the normal
#' auto-commiting mode into transactional mode. Here, changes to the database
#' will not be permanent until \code{dbCommit} is called. If the changes are
#' not to be kept around, you can use \code{dbRollback} to undo all the changes
#' since \code{dbTransaction} was called.
#'
#' @param conn
#'        A MonetDB.R database connection, created using
#'        \code{\link[DBI]{dbConnect}} with the
#'        \code{\link[MonetDB.R]{MonetDB.R}} database driver.
#' @return TRUE if the transaction was successful
#'
#' @examples
#' conn <- dbConnect(MonetDB.R(), "monetdb://localhost/acs")
#' dbSendUpdate(conn, "CREATE TABLE foo(a INT,b VARCHAR(100))")
#' dbTransaction(conn)
#' dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 42, "bar")
#' dbCommit(conn)
#' dbTransaction(conn)
#' dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 43, "bar")
#' dbRollback(conn)
#' @export
#' @rdname transactions
setMethod(
  "dbTransaction", signature(conn = "MonetDBConnection"),
  function(conn, ...) {
    dbBegin(conn)
    warning("dbTransaction() is deprecated, use dbBegin() from now.")
    invisible(TRUE)
  }
)
