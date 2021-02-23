#' @include MonetDBConnection.R

#' @title MonetDB transaction management.
#' @description
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
#' library(DBI)
#' if (foundDefaultMonetDBdatabase()) {
#'   conn <- dbConnect(MonetDB.R())
#'
#'   dbBegin(conn)
#'   dbWriteTable(conn, "USarrests", datasets::USArrests, temporary = TRUE,
#'     transaction = FALSE)
#'   DBI::dbGetQuery(conn, 'SELECT count(*) from "USarrests"')
#'   dbCommit(conn)
#'
#'   dbBegin(conn)
#'   dbExecute(conn, 'DELETE from "USarrests" WHERE "Murder" > 1')
#'   DBI::dbGetQuery(conn, 'SELECT count(*) from "USarrests"')
#'   dbRollback(conn)
#'
#'   # Rolling back changes leads to original count
#'   DBI::dbGetQuery(conn, 'SELECT count(*) from "USarrests"')
#'
#'   dbRemoveTable(conn, "USarrests")
#'   dbDisconnect(conn)
#' }
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
